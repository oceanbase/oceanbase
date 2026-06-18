/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#define private public
#define protected public
#define UNITTEST
#include "lib/container/ob_iarray.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/blocksstable/ob_row_generate.h"
#include "observer/ob_service.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/memtable/ob_memtable_iterator.h"
#include "storage/memtable/ob_memtable_mutator.h"

#include "common/cell/ob_cell_reader.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"

#include "storage/ob_i_store.h"
#include "storage/ob_i_table.h"
#include "storage/compaction/ob_sstable_merge_info_mgr.h"
#include "storage/compaction/ob_partition_merge_iter.h"
#include "storage/compaction/ob_partition_merger.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/blocksstable/ob_multi_version_sstable_test.h"
#include "storage/mockcontainer/mock_ob_merge_iterator.h"

#include "storage/memtable/utils_rowkey_builder.h"
#include "storage/memtable/utils_mock_row.h"
#include "storage/tx/ob_mock_tx_ctx.h"
#include "storage/init_basic_struct.h"
#include "storage/test_tablet_helper.h"
#include "storage/tx_table/ob_tx_table.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_trans_ctx_mgr_v4.h"
#include "storage/tx/ob_tx_data_define.h"
#include "share/scn.h"
#include "src/storage/column_store/ob_column_oriented_sstable.h"
#include "test_merge_basic.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace blocksstable;
using namespace compaction;
using namespace memtable;
using namespace observer;
using namespace unittest;
using namespace memtable;
using namespace transaction;
using namespace palf;

static int64_t MAX_TRANS_VERSION = INT64_MAX - 2;
static ObMergeEngineType original_merge_engine = ObMergeEngineType::OB_MERGE_ENGINE_MAX;
static bool adjust_row_merge_engine_type = false;

static void set_allow_unknown_row(const ObMergeEngineType merge_engine_type)
{
  original_merge_engine = merge_engine_type;
  adjust_row_merge_engine_type = true;
}
// Override adjust_sql_sequence method in ObMacroBlockWriter for testing
namespace blocksstable
{
int ObMacroBlockWriter::adjust_sql_sequence(const ObDatumRow &row,
                                            const int64_t sql_sequence_col_idx,
                                            const bool is_delete_insert_merge,
                                            int64_t &cur_sql_sequence)
{
  int ret = OB_SUCCESS;
  LOG_INFO("adjust_sql_sequence", K(original_merge_engine), K(adjust_row_merge_engine_type), K(row));
  if (data_store_desc_->decide_merge_by_row() && !ObMergeEngineStoreFormat::is_merge_engine_valid(row.merge_engine_type_) && adjust_row_merge_engine_type) {
    // convert max row to unknown row
    const_cast<ObDatumRow&>(row).merge_engine_type_ = ObMergeEngineType::OB_MERGE_ENGINE_UNKNOWN;
    const_cast<bool&>(is_delete_insert_merge) = ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT == original_merge_engine;
  }
  if (!row.mvcc_row_flag_.is_shadow_row()) {
    const_cast<ObDatumRow&>(row).storage_datums_[sql_sequence_col_idx].reuse(); // make sql sequence positive
    if (is_delete_insert_merge && row.row_flag_.is_insert()) {
      const_cast<ObDatumRow&>(row).storage_datums_[sql_sequence_col_idx].set_int(-common::DELETE_INSERT_TRANS_SEQUENCE); // make sql sequence positive
      cur_sql_sequence = -common::DELETE_INSERT_TRANS_SEQUENCE;
    } else {
      const_cast<ObDatumRow&>(row).storage_datums_[sql_sequence_col_idx].set_int(0); // make sql sequence positive
      cur_sql_sequence = 0;
    }
  } else if (OB_UNLIKELY(row.storage_datums_[sql_sequence_col_idx].get_int() != -INT64_MAX)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected shadow row", K(ret), K(row));
  }
  return ret;
}
} // namespace blocksstable

namespace storage
{

class TestMixedRowMinorMerge : public TestMergeBasic
{
public:
  static const int64_t MAX_PARALLEL_DEGREE = 10;
  TestMixedRowMinorMerge();
  virtual ~TestMixedRowMinorMerge() {}

  void SetUp();
  void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
  void prepare_query_param(const ObVersionRange &version_range);
  void prepare_txn(ObStoreCtx *store_ctx, const int64_t prepare_version);

  void prepare_merge_context(const ObMergeType &merge_type,
                             const bool is_full_merge,
                             const ObVersionRange &trans_version_range,
                             ObTabletMergeCtx &merge_context,
                             const ObMergeEngineType merge_engine_type);
  void build_sstable(
      ObTabletMergeCtx &ctx,
      ObSSTable *&merged_sstable);
  void fake_freeze_info();
  void get_tx_table_guard(ObTxTableGuard &tx_table_guard);

public:
  ObStoreCtx store_ctx_;
  ObTabletMergeExecuteDag merge_dag_;
};

void TestMixedRowMinorMerge::SetUpTestCase()
{
  ObMultiVersionSSTableTest::SetUpTestCase();
  // mock sequence no
  ObClockGenerator::init();

  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  MTL(ObTenantTabletScheduler*)->resume_major_merge();

  // create tablet
  share::schema::ObTableSchema table_schema;
  uint64_t table_id = 12345;
  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, table_id));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema, allocator_));
}

void TestMixedRowMinorMerge::TearDownTestCase()
{
  clear_tx_data();
  ObMultiVersionSSTableTest::TearDownTestCase();
  // reset sequence no
  ObClockGenerator::destroy();
}

TestMixedRowMinorMerge::TestMixedRowMinorMerge()
  : TestMergeBasic("test_mixed_row_minor_merge")
{}

void TestMixedRowMinorMerge::SetUp()
{
  ObMultiVersionSSTableTest::SetUp();
}

void TestMixedRowMinorMerge::fake_freeze_info()
{
  share::ObFreezeInfoList &info_list = MTL(ObTenantFreezeInfoMgr *)->freeze_info_mgr_.freeze_info_;
  info_list.reset();

  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, info_list.frozen_statuses_.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));
  frozen_val.val_ = 100;
  ASSERT_EQ(OB_SUCCESS, info_list.frozen_statuses_.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));
  frozen_val.val_ = 200;
  ASSERT_EQ(OB_SUCCESS, info_list.frozen_statuses_.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));
  frozen_val.val_ = 400;
  ASSERT_EQ(OB_SUCCESS, info_list.frozen_statuses_.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  info_list.latest_snapshot_gc_scn_.val_ = 500;
}

void TestMixedRowMinorMerge::TearDown()
{
  ObMultiVersionSSTableTest::TearDown();
  TRANS_LOG(INFO, "teardown success");
}

void TestMixedRowMinorMerge::prepare_txn(ObStoreCtx *store_ctx,
                                        const int64_t prepare_version)
{
  share::SCN prepare_scn;
  prepare_scn.convert_for_tx(prepare_version);
  ObPartTransCtx *tx_ctx = store_ctx->mvcc_acc_ctx_.tx_ctx_;
  ObMemtableCtx *mt_ctx = store_ctx->mvcc_acc_ctx_.mem_ctx_;
  tx_ctx->exec_info_.state_ = ObTxState::PREPARE;
  tx_ctx->exec_info_.prepare_version_ = prepare_scn;
  mt_ctx->trans_version_ = prepare_scn;
}

void TestMixedRowMinorMerge::prepare_query_param(const ObVersionRange &version_range)
{
  context_.reset();
  ObLSID ls_id(ls_id_);
  iter_param_.table_id_ = table_id_;
  iter_param_.tablet_id_ = tablet_id_;
  iter_param_.read_info_ = &full_read_info_;
  iter_param_.out_cols_project_ = nullptr;
  iter_param_.is_same_schema_column_ = true;
  iter_param_.has_virtual_columns_ = false;
  iter_param_.vectorized_enabled_ = false;
  iter_param_.merge_engine_type_ = ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT;
  ASSERT_EQ(OB_SUCCESS,
            store_ctx_.init_for_read(ls_id,
                                     iter_param_.tablet_id_,
                                     INT64_MAX, // query_expire_ts
                                     -1, // lock_timeout_us
                                     share::SCN::max_scn()));
  ObQueryFlag query_flag(ObQueryFlag::Forward,
                         true, /*is daily merge scan*/
                         true, /*is read multiple macro block*/
                         true, /*sys task scan, read one macro block in single io*/
                         false /*full row scan flag, obsoleted*/,
                         false,/*index back*/
                         false); /*query_stat*/
  query_flag.set_not_use_row_cache();
  query_flag.set_not_use_block_cache();
  query_flag.set_iter_uncommitted_row();
  ASSERT_EQ(OB_SUCCESS,
            context_.init(query_flag,
                          store_ctx_,
                          allocator_,
                          allocator_,
                          version_range));
  context_.limit_param_ = nullptr;
}

void TestMixedRowMinorMerge::prepare_merge_context(const ObMergeType &merge_type,
                                                  const bool is_full_merge,
                                                  const ObVersionRange &trans_version_range,
                                                  ObTabletMergeCtx &merge_context,
                                                  const ObMergeEngineType merge_engine_type)
{
  TestMergeBasic::prepare_merge_context(
      merge_type, is_full_merge, trans_version_range, &merge_dag_,merge_context, merge_engine_type);
}

void TestMixedRowMinorMerge::build_sstable(
    ObTabletMergeCtx &ctx,
    ObSSTable *&merged_sstable)
{
  bool tmp_bool = false; // placeholder
  ASSERT_EQ(OB_SUCCESS, ctx.merge_info_.create_sstable(ctx, ctx.merged_table_handle_, tmp_bool));
  ASSERT_EQ(OB_SUCCESS, ctx.merged_table_handle_.get_sstable(merged_sstable));
}

void TestMixedRowMinorMerge::get_tx_table_guard(ObTxTableGuard &tx_table_guard)
{
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tx_table_guard(tx_table_guard));
}

// Test case with merge_engine_type column
TEST_F(TestMixedRowMinorMerge, test_all_delete_insert_merge)
{
  int ret = OB_SUCCESS;
  ObMergeEngineType original_merge_engine_type = ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint  var   bigint  bigint     bigint  bigint  flag    flag_type  multi_version_row_flag  merge_engine_type\n"
      "1       var1  -20     MIN        9       9       INSERT  NORMAL     SCF                     di\n"
      "1       var1  -20     DI_VERSION 9       9       INSERT  NORMAL     C                       di\n"
      "1       var1  -20     0          9       1       DELETE  NORMAL     C                       di\n"
      "1       var1  -10     DI_VERSION 9       1       INSERT  NORMAL     C                       di\n"
      "1       var1  -10     0          1       1       DELETE  NORMAL     C                       di\n"
      "1       var1  -8      0          1       1       INSERT  NORMAL     CL                      di\n"
      "2       var2  -20     0          2       2       INSERT  NORMAL     CLF                     di\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 30;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, original_merge_engine_type);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1, false, false);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint  var   bigint  bigint     bigint  bigint  flag    flag_type  multi_version_row_flag  merge_engine_type\n"
      "2       var2  -50     DI_VERSION 9       9       INSERT  NORMAL     CF                      di\n"
      "2       var2  -50     0          2       2       DELETE  NORMAL     CL                      di\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1, false, false);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint  var   bigint  bigint     bigint  bigint  flag    flag_type  multi_version_row_flag  merge_engine_type\n"
      "1       var1  -80     MIN        19      19      INSERT  NORMAL     SCF                     di\n"
      "1       var1  -80     DI_VERSION 19      19      INSERT  NORMAL     C                       di\n"
      "1       var1  -80     0          19      9       DELETE  NORMAL     C                       di\n"
      "1       var1  -60     DI_VERSION 19      9       INSERT  NORMAL     C                       di\n"
      "1       var1  -60     0          9       9       DELETE  NORMAL     CL                      di\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(80);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1, false, false);
  prepare_data_end(handle3);
  merge_context.static_param_.tables_handle_.add_table(handle3);
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 3;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context, original_merge_engine_type);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint  var   bigint  bigint     bigint  bigint  flag    flag_type  multi_version_row_flag  merge_engine_type\n"
      "1       var1  -80     MIN        19      19      INSERT  NORMAL     SCF                     di\n"
      "1       var1  -80     DI_VERSION 19      19      INSERT  NORMAL     C                       di\n"
      "1       var1  -80     0          19      9       DELETE  NORMAL     C                       di\n"
      "1       var1  -60     DI_VERSION 19      9       INSERT  NORMAL     C                       di\n"
      "1       var1  -60     0          9       9       DELETE  NORMAL     C                       di\n"
      "1       var1  -20     DI_VERSION 9       9       INSERT  NORMAL     C                       di\n"
      "1       var1  -20     0          9       1       DELETE  NORMAL     C                       di\n"
      "1       var1  -10     DI_VERSION 9       1       INSERT  NORMAL     C                       di\n"
      "1       var1  -10     0          1       1       DELETE  NORMAL     C                       di\n"
      "1       var1  -8      DI_VERSION 1       1       INSERT  NORMAL     CL                      di\n"
      "2       var2  -50     MIN        9       9       INSERT  NORMAL     SCF                     di\n"
      "2       var2  -50     DI_VERSION 9       9       INSERT  NORMAL     C                       di\n"
      "2       var2  -50     0          2       2       DELETE  NORMAL     C                       di\n"
      "2       var2  -20     DI_VERSION 2       2       INSERT  NORMAL     CL                      di\n";

  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_query_param(trans_version_range);
  ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param_, context_, range, scanner));
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  ObMockDirectReadIterator sstable_iter;
  ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, full_read_info_));
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true, false, false, false, true);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  handle3.reset();
  merger.reset();
  ASSERT_TRUE(is_equal);
}

TEST_F(TestMixedRowMinorMerge, test_all_partial_update_merge)
{
  int ret = OB_SUCCESS;
  ObMergeEngineType original_merge_engine_type = ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[4];
  micro_data[0] =
      "bigint  var   bigint  bigint  bigint  bigint  flag    multi_version_row_flag  merge_engine_type\n"
      "0       var1  -8      0       NOP    1       UPDATE  LF                      pu\n"
      "1       var1  -8      MIN     3      3       UPDATE  SCF                     pu\n"
      "1       var1  -8      0       3      NOP     UPDATE  N                       pu\n";

  micro_data[1] =
      "bigint  var   bigint  bigint  bigint  bigint  flag    multi_version_row_flag  merge_engine_type\n"
      "1       var1  -7      0       NOP    3       UPDATE  N                       pu\n";

  micro_data[2] =
      "bigint  var   bigint  bigint  bigint  bigint  flag    multi_version_row_flag  merge_engine_type\n"
      "1       var1  -6      0       NOP    2       UPDATE  N                       pu\n";

  micro_data[3] =
      "bigint  var   bigint  bigint  bigint  bigint  flag    multi_version_row_flag  merge_engine_type\n"
      "1       var1  -5      0       2      NOP     UPDATE   L                       pu\n"
      "2       var1  -5      0       2      2       INSERT  CLF                      pu\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1, false, false);
  prepare_one_macro(&micro_data[1], 1, false, false);
  prepare_one_macro(&micro_data[2], 1, false, false);
  prepare_one_macro(&micro_data[3], 1, false, false);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint  var   bigint  bigint  bigint  bigint  flag    multi_version_row_flag  merge_engine_type\n"
      "0       var1  -10     0       NOP    10      UPDATE  LF                      pu\n"
      "2       var1  -10     0       NOP    12      UPDATE  LF                      pu\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1, false, false);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context, original_merge_engine_type);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint  var   bigint  bigint  bigint  bigint  flag    multi_version_row_flag  merge_engine_type\n"
      "0       var1  -10     MIN     NOP    10      UPDATE  SF                      pu\n"
      "0       var1  -10     0       NOP    10      UPDATE  N                       pu\n"
      "0       var1  -8      0       NOP    1       UPDATE  L                       pu\n"
      "1       var1  -8      MIN     3      3       UPDATE  SCF                     pu\n"
      "1       var1  -8      0       3      NOP     UPDATE  N                       pu\n"
      "1       var1  -7      0       NOP    3       UPDATE  N                       pu\n"
      "1       var1  -6      0       NOP    2       UPDATE  N                       pu\n"
      "1       var1  -5      0       2      NOP     UPDATE  L                       pu\n"
      "2       var1  -10     MIN     2      12      UPDATE  SCF                     pu\n"
      "2       var1  -10     0       NOP    12      UPDATE  N                       pu\n"
      "2       var1  -5      0       2      2       INSERT  CL                      pu\n";

  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_query_param(trans_version_range);

  ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param_, context_, range, scanner));
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  ObMockDirectReadIterator sstable_iter;
  ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, full_read_info_));
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true, false, false, false, true);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
  ASSERT_TRUE(is_equal);
}

TEST_F(TestMixedRowMinorMerge, test_mixed_merge)
{
  int ret = OB_SUCCESS;
  ObMergeEngineType original_merge_engine_type = ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint  var   bigint  bigint     bigint  bigint  flag    flag_type  multi_version_row_flag  merge_engine_type\n"
      "1       var1  -20     MIN        9       9       INSERT  NORMAL     SCF                     di\n"
      "1       var1  -20     DI_VERSION 9       9       INSERT  NORMAL     C                       di\n"
      "1       var1  -20     0          9       1       DELETE  NORMAL     C                       di\n"
      "1       var1  -7      0          9       NOP     UPDATE  NORMAL     N                       pu\n"
      "1       var1  -6      0          -1      1       INSERT  NORMAL     CL                      pu\n"
      "2       var2  -10     0          2       2       INSERT  NORMAL     CLF                     di\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 20;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(20);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, original_merge_engine_type);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1, false, false);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint  var   bigint  bigint     bigint  bigint  flag    flag_type  multi_version_row_flag  merge_engine_type\n"
      "2       var2  -60     MIN        10      10      UPDATE  NORMAL     SCF                     pu\n"
      "2       var2  -60     0          NOP     10      UPDATE  NORMAL     N                       pu\n"
      "2       var2  -50     0          10      NOP     UPDATE  NORMAL     N                       pu\n"
      "2       var2  -40     0          9       9       INSERT  NORMAL     C                       pu\n"
      "2       var2  -30     0          NOP     NOP     DELETE  NORMAL     N                       pu\n"
      "2       var2  -20     DI_VERSION 3       3       INSERT  NORMAL     C                       di\n"
      "2       var2  -20     0          2       2       DELETE  NORMAL     CL                      di\n";

  snapshot_version = 60;
  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(60);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1, false, false);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint  var   bigint  bigint     bigint  bigint  flag    flag_type  multi_version_row_flag  merge_engine_type\n"
      "1       var1  -80     MIN        19      19      INSERT  NORMAL     SCF                     di\n"
      "1       var1  -80     DI_VERSION 19      19      INSERT  NORMAL     C                       di\n"
      "1       var1  -80     0          19      9       DELETE  NORMAL     C                       di\n"
      "1       var1  -70     DI_VERSION 19      9       INSERT  NORMAL     C                       di\n"
      "1       var1  -70     0          9       9       DELETE  NORMAL     CL                      di\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(70);
  scn_range.end_scn_.convert_for_tx(80);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1, false, false);
  prepare_data_end(handle3);
  merge_context.static_param_.tables_handle_.add_table(handle3);
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 3;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context, original_merge_engine_type);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint  var   bigint  bigint     bigint  bigint  flag    flag_type  multi_version_row_flag  merge_engine_type\n"
      "1       var1  -80     MIN        19      19      INSERT  NORMAL     SCF                     di\n"
      "1       var1  -80     DI_VERSION 19      19      INSERT  NORMAL     C                       di\n"
      "1       var1  -80     0          19      9       DELETE  NORMAL     C                       di\n"
      "1       var1  -70     DI_VERSION 19      9       INSERT  NORMAL     C                       di\n"
      "1       var1  -70     0          9       9       DELETE  NORMAL     C                       di\n"
      "1       var1  -20     DI_VERSION 9       9       INSERT  NORMAL     C                       di\n"
      "1       var1  -20     0          9       1       DELETE  NORMAL     C                       di\n"
      "1       var1  -7      0          9       NOP     UPDATE  NORMAL     N                       pu\n"
      "1       var1  -6      0          -1      1       INSERT  NORMAL     CL                      pu\n"
      "2       var2  -60     MIN        10      10      UPDATE  NORMAL     SCF                     pu\n"
      "2       var2  -60     0          NOP     10      UPDATE  NORMAL     N                       pu\n"
      "2       var2  -50     0          10      NOP     UPDATE  NORMAL     N                       pu\n"
      "2       var2  -40     0          9       9       INSERT  NORMAL     C                       pu\n"
      "2       var2  -30     0          NOP     NOP     DELETE  NORMAL     C                       pu\n"
      "2       var2  -20     DI_VERSION 3       3       INSERT  NORMAL     C                       di\n"
      "2       var2  -20     0          2       2       DELETE  NORMAL     C                       di\n"
      "2       var2  -10     DI_VERSION 2       2       INSERT  NORMAL     CL                      di\n";

  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_query_param(trans_version_range);
  ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param_, context_, range, scanner));
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  ObMockDirectReadIterator sstable_iter;
  ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, full_read_info_));
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true, false, false, false, true);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  handle3.reset();
  merger.reset();
  ASSERT_TRUE(is_equal);
}

TEST_F(TestMixedRowMinorMerge, test_mixed_merge_with_original_delete_insert)
{
  int ret = OB_SUCCESS;
  ObMergeEngineType original_merge_engine_type = ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint  var   bigint  bigint     bigint  bigint  flag    flag_type  multi_version_row_flag  merge_engine_type\n"
      "1       var1  -20     MIN        9       9       INSERT  NORMAL     SCF                     di\n"
      "1       var1  -20     DI_VERSION 9       9       INSERT  NORMAL     C                       di\n"
      "1       var1  -20     0          9       1       DELETE  NORMAL     C                       di\n"
      "1       var1  -9      DI_VERSION 9       1       INSERT  NORMAL     C                       di\n"
      "1       var1  -9      0          1       1       DELETE  NORMAL     CL                      di\n"
      "2       var2  -10     0          2       2       INSERT  NORMAL     CLF                     di\n"
      "3       var3  -10     MIN        3       3       INSERT  NORMAL     SCF                     di\n"
      "3       var3  -10     DI_VERSION 3       3       INSERT  NORMAL     C                       di\n"
      "3       var3  -10     0          1       1       DELETE  NORMAL     CL                      di\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 20;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(20);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, original_merge_engine_type);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1, false, false);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint  var   bigint  bigint     bigint  bigint  flag    flag_type  multi_version_row_flag  merge_engine_type\n"
      "2       var2  -60     MIN        10      10      UPDATE  NORMAL     SCF                     pu\n"
      "2       var2  -60     0          NOP     10      UPDATE  NORMAL     N                       pu\n"
      "2       var2  -50     0          10      NOP     UPDATE  NORMAL     N                       pu\n"
      "2       var2  -40     0          9       9       INSERT  NORMAL     C                       pu\n"
      "2       var2  -30     0          NOP     NOP     DELETE  NORMAL     N                       pu\n"
      "2       var2  -20     DI_VERSION 3       3       INSERT  NORMAL     C                       di\n"
      "2       var2  -20     0          2       2       DELETE  NORMAL     CL                      di\n";

  snapshot_version = 60;
  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(60);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1, false, false);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint  var   bigint  bigint     bigint  bigint  flag    flag_type  multi_version_row_flag  merge_engine_type\n"
      "1       var1  -80     MIN        19      19      INSERT  NORMAL     SCF                     di\n"
      "1       var1  -80     DI_VERSION 19      19      INSERT  NORMAL     C                       di\n"
      "1       var1  -80     0          9       9       DELETE  NORMAL     CL                      di\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(60);
  scn_range.end_scn_.convert_for_tx(80);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1, false, false);
  prepare_data_end(handle3);
  merge_context.static_param_.tables_handle_.add_table(handle3);
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 3;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context, original_merge_engine_type);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint  var   bigint  bigint     bigint  bigint  flag    flag_type  multi_version_row_flag  merge_engine_type\n"
      "1       var1  -80     MIN        19      19      INSERT  NORMAL     SCF                     di\n"
      "1       var1  -80     DI_VERSION 19      19      INSERT  NORMAL     C                       di\n"
      "1       var1  -80     0          9       9       DELETE  NORMAL     C                       di\n"
      "1       var1  -20     DI_VERSION 9       9       INSERT  NORMAL     C                       di\n"
      "1       var1  -20     0          9       1       DELETE  NORMAL     C                       di\n"
      "1       var1  -9      DI_VERSION 9       1       INSERT  NORMAL     C                       di\n"
      "1       var1  -9      0          1       1       DELETE  NORMAL     CL                      di\n"
      "2       var2  -60     MIN        10      10      UPDATE  NORMAL     SCF                     pu\n"
      "2       var2  -60     0          NOP     10      UPDATE  NORMAL     N                       pu\n"
      "2       var2  -50     0          10      NOP     UPDATE  NORMAL     N                       pu\n"
      "2       var2  -40     0          9       9       INSERT  NORMAL     C                       pu\n"
      "2       var2  -30     0          NOP     NOP     DELETE  NORMAL     C                       pu\n"
      "2       var2  -20     DI_VERSION 3       3       INSERT  NORMAL     C                       di\n"
      "2       var2  -20     0          2       2       DELETE  NORMAL     C                       di\n"
      "2       var2  -10     DI_VERSION 2       2       INSERT  NORMAL     CL                      di\n"
      "3       var3  -10     MIN        3       3       INSERT  NORMAL     SCF                     di\n"
      "3       var3  -10     DI_VERSION 3       3       INSERT  NORMAL     C                       di\n"
      "3       var3  -10     0          1       1       DELETE  NORMAL     CL                      di\n";

  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_query_param(trans_version_range);
  ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param_, context_, range, scanner));
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  ObMockDirectReadIterator sstable_iter;
  ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, full_read_info_));
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true, false, false, false, true);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  handle3.reset();
  merger.reset();
  ASSERT_TRUE(is_equal);
}

TEST_F(TestMixedRowMinorMerge, test_mixed_merge_with_original_partial_update)
{
  int ret = OB_SUCCESS;
  ObMergeEngineType original_merge_engine_type = ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint  var   bigint  bigint     bigint  bigint  flag    flag_type  multi_version_row_flag  merge_engine_type\n"
      "1       var1  -20     MIN        9       9       UPDATE  NORMAL     SCF                     pu\n"
      "1       var1  -20     0          9       9       UPDATE  NORMAL     C                       pu\n"
      "1       var1  -10     0          1       1       INSERT  NORMAL     C                       pu\n"
      "1       var1  -9      0          NOP     NOP     DELETE  NORMAL     L                       pu\n"
      "2       var2  -10     0          2       2       INSERT  NORMAL     CLF                     pu\n"
      "3       var3  -13     MIN        4       3       UPDATE  NORMAL     SCF                     pu\n"
      "3       var3  -13     0          4       NOP     UPDATE  NORMAL     N                       pu\n"
      "3       var3  -12     0          3       3       INSERT  NORMAL     C                       pu\n"
      "3       var3  -11     0          NOP     NOP     DELETE  NORMAL     N                       pu\n"
      "3       var3  -10     0          1       1       INSERT  NORMAL     CL                      pu\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 20;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(20);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, original_merge_engine_type);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1, false, false);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint  var   bigint  bigint     bigint  bigint  flag    flag_type  multi_version_row_flag  merge_engine_type\n"
      "2       var2  -60     MIN        10      10      UPDATE  NORMAL     SCF                     pu\n"
      "2       var2  -60     0          NOP     10      UPDATE  NORMAL     N                       pu\n"
      "2       var2  -50     0          10      NOP     UPDATE  NORMAL     N                       pu\n"
      "2       var2  -40     0          9       9       INSERT  NORMAL     C                       pu\n"
      "2       var2  -30     0          NOP     NOP     DELETE  NORMAL     N                       pu\n"
      "2       var2  -20     0          NOP     3       UPDATE  NORMAL     N                       pu\n"
      "2       var2  -15     0          3       NOP     UPDATE  NORMAL     L                       pu\n";

  snapshot_version = 60;
  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(60);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1, false, false);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint  var   bigint  bigint     bigint  bigint  flag    flag_type  multi_version_row_flag  merge_engine_type\n"
      "1       var1  -80     MIN        19      19      INSERT  NORMAL     SCF                     di\n"
      "1       var1  -80     DI_VERSION 19      19      INSERT  NORMAL     C                       di\n"
      "1       var1  -80     0          9       9       DELETE  NORMAL     CL                      di\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(60);
  scn_range.end_scn_.convert_for_tx(80);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1, false, false);
  prepare_data_end(handle3);
  merge_context.static_param_.tables_handle_.add_table(handle3);
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 3;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context, original_merge_engine_type);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint  var   bigint  bigint     bigint  bigint  flag    flag_type  multi_version_row_flag  merge_engine_type\n"
      "1       var1  -80     MIN        19      19      INSERT  NORMAL     SCF                     di\n"
      "1       var1  -80     DI_VERSION 19      19      INSERT  NORMAL     C                       di\n"
      "1       var1  -80     0          9       9       DELETE  NORMAL     C                       di\n"
      "1       var1  -20     0          9       9       UPDATE  NORMAL     C                       pu\n"
      "1       var1  -10     0          1       1       INSERT  NORMAL     C                       pu\n"
      "1       var1  -9      0          NOP     NOP     DELETE  NORMAL     CL                      pu\n"
      "2       var2  -60     MIN        10      10      UPDATE  NORMAL     SCF                     pu\n"
      "2       var2  -60     0          NOP     10      UPDATE  NORMAL     N                       pu\n"
      "2       var2  -50     0          10      NOP     UPDATE  NORMAL     N                       pu\n"
      "2       var2  -40     0          9       9       INSERT  NORMAL     C                       pu\n"
      "2       var2  -30     0          NOP     NOP     DELETE  NORMAL     C                       pu\n"
      "2       var2  -20     0          NOP     3       UPDATE  NORMAL     N                       pu\n"
      "2       var2  -15     0          3       NOP     UPDATE  NORMAL     N                       pu\n"
      "2       var2  -10     0          2       2       INSERT  NORMAL     CL                      pu\n"
      "3       var3  -13     MIN        4       3       UPDATE  NORMAL     SCF                     pu\n"
      "3       var3  -13     0          4       NOP     UPDATE  NORMAL     N                       pu\n"
      "3       var3  -12     0          3       3       INSERT  NORMAL     C                       pu\n"
      "3       var3  -11     0          NOP     NOP     DELETE  NORMAL     C                       pu\n"
      "3       var3  -10     0          1       1       INSERT  NORMAL     CL                      pu\n";

  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_query_param(trans_version_range);
  ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param_, context_, range, scanner));
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  ObMockDirectReadIterator sstable_iter;
  ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, full_read_info_));
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true, false, false, false, true);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  handle3.reset();
  merger.reset();
  ASSERT_TRUE(is_equal);
}

TEST_F(TestMixedRowMinorMerge, test_mixed_merge_with_recycle)
{
  int ret = OB_SUCCESS;
  ObMergeEngineType original_merge_engine_type = ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint  var   bigint  bigint     bigint  bigint  flag    flag_type  multi_version_row_flag  merge_engine_type\n"
      "1       var1  -20     MIN        9       9       INSERT  NORMAL     SCF                     di\n"
      "1       var1  -20     DI_VERSION 9       9       INSERT  NORMAL     C                       di\n"
      "1       var1  -20     0          9       1       DELETE  NORMAL     C                       di\n"
      "1       var1  -10     DI_VERSION 9       1       INSERT  NORMAL     C                       di\n"
      "1       var1  -10     0          7       1       DELETE  NORMAL     C                       di\n"
      "1       var1  -7      0          7       NOP     UPDATE  NORMAL     N                       pu\n"
      "1       var1  -6      0          -1      1       INSERT  NORMAL     CL                      pu\n"
      "2       var2  -4      MIN        2       2       INSERT  NORMAL     SCF                     di\n"
      "2       var2  -4      DI_VERSION 2       2       INSERT  NORMAL     C                       di\n"
      "2       var2  -4      0          1       1       DELETE  NORMAL     C                       di\n"
      "2       var2  -3      DI_VERSION 1       1       INSERT  NORMAL     C                       di\n"
      "2       var2  -3      0          0       0       DELETE  NORMAL     CL                      di\n"
      "3       var3  -9      MIN        3       3       UPDATE  NORMAL     SCF                     pu\n"
      "3       var3  -9      0          NOP     3       UPDATE  NORMAL     N                       pu\n"
      "3       var3  -8      0          3       NOP     UPDATE  NORMAL     N                       pu\n"
      "3       var3  -5      0          1       1       INSERT  NORMAL     CL                      pu\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 20;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(20);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, original_merge_engine_type);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1, false, false);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint  var   bigint  bigint     bigint  bigint  flag    flag_type  multi_version_row_flag  merge_engine_type\n"
      "2       var2  -60     MIN        10      10      INSERT  NORMAL     SCF                     di\n"
      "2       var2  -60     DI_VERSION 10      10      INSERT  NORMAL     C                       di\n"
      "2       var2  -60     0          6       6       DELETE  NORMAL     C                       di\n"
      "2       var2  -40     DI_VERSION 6       6       INSERT  NORMAL     C                       di\n"
      "2       var2  -40     0          9       9       DELETE  NORMAL     CL                      di\n";

  snapshot_version = 60;
  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(60);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1, false, false);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint  var   bigint  bigint     bigint  bigint  flag    flag_type  multi_version_row_flag  merge_engine_type\n"
      "1       var1  -80     MIN        19      19      INSERT  NORMAL     SCF                     di\n"
      "1       var1  -80     DI_VERSION 19      19      INSERT  NORMAL     C                       di\n"
      "1       var1  -80     0          9       9       DELETE  NORMAL     CL                      di\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(70);
  scn_range.end_scn_.convert_for_tx(80);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1, false, false);
  prepare_data_end(handle3);
  merge_context.static_param_.tables_handle_.add_table(handle3);
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context, original_merge_engine_type);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint  var   bigint  bigint     bigint  bigint  flag    flag_type  multi_version_row_flag  merge_engine_type\n"
      "1       var1  -80     MIN        19      19      INSERT  NORMAL     SCF                     di\n"
      "1       var1  -80     DI_VERSION 19      19      INSERT  NORMAL     C                       di\n"
      "1       var1  -80     0          9       9       DELETE  NORMAL     C                       di\n"
      "1       var1  -20     DI_VERSION 9       9       INSERT  NORMAL     C                       di\n"
      "1       var1  -10     0          7       1       DELETE  NORMAL     CL                      di\n"
      "2       var2  -60     MIN        10      10      INSERT  NORMAL     SCF                     di\n"
      "2       var2  -60     DI_VERSION 10      10      INSERT  NORMAL     C                       di\n"
      "2       var2  -60     0          6       6       DELETE  NORMAL     C                       di\n"
      "2       var2  -40     DI_VERSION 6       6       INSERT  NORMAL     C                       di\n"
      "2       var2  -40     0          9       9       DELETE  NORMAL     C                       di\n"
      "2       var2  -4      DI_VERSION 2       2       INSERT  NORMAL     C                       di\n"
      "2       var2  -3      0          0       0       DELETE  NORMAL     CL                      di\n"
      "3       var3  -9      0          3       3       UPDATE  NORMAL     CLF                     pu\n";


  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_query_param(trans_version_range);
  ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param_, context_, range, scanner));
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  ObMockDirectReadIterator sstable_iter;
  ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, full_read_info_));
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true, false, false, false, true);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  handle3.reset();
  merger.reset();
  ASSERT_TRUE(is_equal);
}

TEST_F(TestMixedRowMinorMerge, test_mixed_merge_with_uncommitted)
{
  int ret = OB_SUCCESS;
  ObMergeEngineType original_merge_engine_type = ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint  var   bigint  bigint     bigint  bigint  flag    flag_type  multi_version_row_flag  merge_engine_type trans_id\n"
      "1       var1  -20     MIN        9       9       UPDATE  NORMAL     SCF                     pu\n"
      "1       var1  -20     0          9       9       UPDATE  NORMAL     C                       pu\n"
      "1       var1  -10     0          1       1       INSERT  NORMAL     C                       pu\n"
      "1       var1  -9      0          NOP     NOP     DELETE  NORMAL     L                       pu\n"
      "2       var2  -10     0          2       2       INSERT  NORMAL     CLF                     pu\n"
      "3       var3  MIN     -60        2       2       INSERT  NORMAL     UCF                     di                trans_id_1\n"
      "3       var3  MIN     -59        4       3       DELETE  NORMAL     UC                      di                trans_id_1\n"
      "3       var3  -13     MIN        4       3       UPDATE  NORMAL     SC                      pu\n"
      "3       var3  -13     0          4       NOP     UPDATE  NORMAL     N                       pu\n"
      "3       var3  -12     0          3       3       INSERT  NORMAL     C                       pu\n"
      "3       var3  -11     0          NOP     NOP     DELETE  NORMAL     N                       pu\n"
      "3       var3  -10     0          1       1       INSERT  NORMAL     CL                      pu\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 20;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(20);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, original_merge_engine_type);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1, true, false);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint  var   bigint  bigint     bigint  bigint  flag    flag_type  multi_version_row_flag  merge_engine_type trans_id\n"
      "2       var2  MIN     -80        10      11      INSERT  NORMAL     UCF                     di                trans_id_1\n"
      "2       var2  MIN     -79        10      10      DELETE  NORMAL     UC                      di                trans_id_1\n"
      "2       var2  -60     MIN        10      10      UPDATE  NORMAL     SC                      pu\n"
      "2       var2  -60     0          NOP     10      UPDATE  NORMAL     N                       pu\n"
      "2       var2  -50     0          10      NOP     UPDATE  NORMAL     N                       pu\n"
      "2       var2  -40     0          9       9       INSERT  NORMAL     C                       pu\n"
      "2       var2  -30     0          NOP     NOP     DELETE  NORMAL     N                       pu\n"
      "2       var2  -20     0          NOP     3       UPDATE  NORMAL     N                       pu\n"
      "2       var2  -15     0          3       NOP     UPDATE  NORMAL     L                       pu\n";

  snapshot_version = 60;
  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(60);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1, true, false);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint  var   bigint  bigint     bigint  bigint  flag    flag_type  multi_version_row_flag  merge_engine_type trans_id\n"
      "1       var1  MIN     -100        19      19      DELETE  NORMAL     UCF                    di                trans_id_1\n"
      "1       var1  -80     MIN        19      19      INSERT  NORMAL     SC                      di\n"
      "1       var1  -80     DI_VERSION 19      19      INSERT  NORMAL     C                       di\n"
      "1       var1  -80     0          9       9       DELETE  NORMAL     CL                      di\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(60);
  scn_range.end_scn_.convert_for_tx(80);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1, true, false);
  prepare_data_end(handle3);
  merge_context.static_param_.tables_handle_.add_table(handle3);
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  ls_handle.get_ls()->get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  for (int64_t i = 1; i <= 1; i++) {
    ObTxData *tx_data = new ObTxData();
    ASSERT_EQ(OB_SUCCESS, tx_data->init_tx_op());
    transaction::ObTransID tx_id = i;

    // fill in data
    tx_data->tx_id_ = tx_id;
    tx_data->commit_version_.convert_for_tx(INT64_MAX);
    tx_data->start_scn_.convert_for_tx(i);
    tx_data->end_scn_.convert_for_tx(80);
    tx_data->state_ = ObTxData::RUNNING;
    transaction::ObUndoAction undo_action(ObTxSEQ(9, 0),ObTxSEQ(1, 0));
    tx_data->add_undo_action(tx_table, undo_action);

    ASSERT_EQ(OB_SUCCESS, tx_table->insert(tx_data));
    delete tx_data;
  }

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 3;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context, original_merge_engine_type);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint  var   bigint  bigint     bigint  bigint  flag    flag_type  multi_version_row_flag  merge_engine_type trans_id\n"
      "1       var1  MIN     -100       19      19      DELETE  NORMAL     UCF                     di                trans_id_1\n"
      "1       var1  -80     MIN        19      19      INSERT  NORMAL     SC                      di\n"
      "1       var1  -80     DI_VERSION 19      19      INSERT  NORMAL     C                       di\n"
      "1       var1  -80     0          9       9       DELETE  NORMAL     C                       di\n"
      "1       var1  -20     0          9       9       UPDATE  NORMAL     C                       pu\n"
      "1       var1  -10     0          1       1       INSERT  NORMAL     C                       pu\n"
      "1       var1  -9      0          NOP     NOP     DELETE  NORMAL     CL                      pu\n"
      "2       var2  MIN     -80        10      11      INSERT  NORMAL     UCF                     di                trans_id_1\n"
      "2       var2  MIN     -79        10      10      DELETE  NORMAL     UC                      di                trans_id_1\n"
      "2       var2  -60     MIN        10      10      UPDATE  NORMAL     SC                      pu\n"
      "2       var2  -60     0          NOP     10      UPDATE  NORMAL     N                       pu\n"
      "2       var2  -50     0          10      NOP     UPDATE  NORMAL     N                       pu\n"
      "2       var2  -40     0          9       9       INSERT  NORMAL     C                       pu\n"
      "2       var2  -30     0          NOP     NOP     DELETE  NORMAL     C                       pu\n"
      "2       var2  -20     0          NOP     3       UPDATE  NORMAL     N                       pu\n"
      "2       var2  -15     0          3       NOP     UPDATE  NORMAL     N                       pu\n"
      "2       var2  -10     0          2       2       INSERT  NORMAL     CL                      pu\n"
      "3       var3  MIN     -60        2       2       INSERT  NORMAL     UCF                     di                trans_id_1\n"
      "3       var3  MIN     -59        4       3       DELETE  NORMAL     UC                      di                trans_id_1\n"
      "3       var3  -13     MIN        4       3       UPDATE  NORMAL     SC                      pu\n"
      "3       var3  -13     0          4       NOP     UPDATE  NORMAL     N                       pu\n"
      "3       var3  -12     0          3       3       INSERT  NORMAL     C                       pu\n"
      "3       var3  -11     0          NOP     NOP     DELETE  NORMAL     C                       pu\n"
      "3       var3  -10     0          1       1       INSERT  NORMAL     CL                      pu\n";

  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_query_param(trans_version_range);
  ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param_, context_, range, scanner));
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  ObMockDirectReadIterator sstable_iter;
  ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, full_read_info_));
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true, false, false, false, true);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  handle3.reset();
  merger.reset();
  ASSERT_TRUE(is_equal);
}

} // end namespace storage
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_mixed_row_minor_merge.log*");
  OB_LOGGER.set_file_name("test_mixed_row_minor_merge.log");
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
