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
#define private public
#define protected public
#define UNITTEST
#include "lib/container/ob_iarray.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/ob_partition_component_factory.h"
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
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/blocksstable/ob_multi_version_sstable_test.h"

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

namespace storage
{

ObSEArray<ObTxData, 8> TX_DATA_ARR;

int ObTxTable::insert(ObTxData *&tx_data)
{
  int ret = OB_SUCCESS;
  ret = TX_DATA_ARR.push_back(*tx_data);
  return ret;
}

int ObTxTable::check_with_tx_data(ObReadTxDataArg &read_tx_data_arg, ObITxDataCheckFunctor &fn)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < TX_DATA_ARR.count(); i++)
  {
    if (read_tx_data_arg.tx_id_ == TX_DATA_ARR.at(i).tx_id_) {
      ret = fn(TX_DATA_ARR[i]);
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "check with tx data failed", KR(ret), K(read_tx_data_arg), K(TX_DATA_ARR.at(i)));
      }
      break;
    }
  }
  return ret;
}

int clear_tx_data()
{
  TX_DATA_ARR.reset();
  return OB_SUCCESS;
};


class TestMultiVersionMerge : public ObMultiVersionSSTableTest
{
public:
  static const int64_t MAX_PARALLEL_DEGREE = 10;
  TestMultiVersionMerge();
  virtual ~TestMultiVersionMerge() {}

  void SetUp();
  void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
  void prepare_query_param(const ObVersionRange &version_range);

  void prepare_merge_context(const ObMergeType &merge_type,
                             const bool is_full_merge,
                             const ObVersionRange &trans_version_range,
                             ObTabletMergeCtx &merge_context);
  void build_sstable(
      ObTabletMergeCtx &ctx,
      ObSSTable *&merged_sstable);
  void fake_freeze_info();

public:
  ObStorageSchema table_merge_schema_;
  ObStoreCtx store_ctx_;
};

void TestMultiVersionMerge::SetUpTestCase()
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

void TestMultiVersionMerge::TearDownTestCase()
{
  ObMultiVersionSSTableTest::TearDownTestCase();
  // reset sequence no
  ObClockGenerator::destroy();
}

TestMultiVersionMerge::TestMultiVersionMerge()
  : ObMultiVersionSSTableTest("test_multi_version_merge")
{}

void TestMultiVersionMerge::SetUp()
{
  ObMultiVersionSSTableTest::SetUp();
}

void TestMultiVersionMerge::fake_freeze_info()
{
  common::ObArray<ObTenantFreezeInfoMgr::FreezeInfo> freeze_info;
  common::ObArray<share::ObSnapshotInfo> snapshots;

  const int64_t snapshot_gc_ts = 500;
  bool changed = false;

  share::SCN scn;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(ObTenantFreezeInfoMgr::FreezeInfo(1, 1, 0)));
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(ObTenantFreezeInfoMgr::FreezeInfo(100, 1, 0)));
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(ObTenantFreezeInfoMgr::FreezeInfo(200, 1, 0)));
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(ObTenantFreezeInfoMgr::FreezeInfo(400, 1, 0)));

  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantFreezeInfoMgr *)->update_info(
        snapshot_gc_ts,
        freeze_info,
        snapshots,
        INT64_MAX,
        changed));
}

void TestMultiVersionMerge::TearDown()
{

  ObMultiVersionSSTableTest::TearDown();
  TRANS_LOG(INFO, "teardown success");
}

void TestMultiVersionMerge::prepare_query_param(const ObVersionRange &version_range)
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
  ASSERT_EQ(OB_SUCCESS,
            store_ctx_.init_for_read(ls_id,
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
  //query_flag.multi_version_minor_merge_ = true;
  ASSERT_EQ(OB_SUCCESS,
            context_.init(query_flag,
                          store_ctx_,
                          allocator_,
                          allocator_,
                          version_range));
  context_.limit_param_ = nullptr;
}

void TestMultiVersionMerge::prepare_merge_context(const ObMergeType &merge_type,
                                                  const bool is_full_merge,
                                                  const ObVersionRange &trans_version_range,
                                                  ObTabletMergeCtx &merge_context)
{
  bool has_lob = false;
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  merge_context.ls_handle_ = ls_handle;

  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
  merge_context.tablet_handle_ = tablet_handle;

  table_merge_schema_.reset();
  OK(table_merge_schema_.init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
  merge_context.schema_ctx_.base_schema_version_ = table_schema_.get_schema_version();
  merge_context.schema_ctx_.schema_version_ = table_schema_.get_schema_version();
  merge_context.schema_ctx_.storage_schema_ = &table_merge_schema_;

  merge_context.is_full_merge_ = is_full_merge;
  merge_context.merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
  merge_context.param_.merge_type_ = merge_type;
  merge_context.param_.merge_version_ = 0;
  merge_context.param_.ls_id_ = ls_id_;
  merge_context.param_.tablet_id_ = tablet_id_;
  merge_context.sstable_version_range_ = trans_version_range;
  merge_context.param_.report_ = &rs_reporter_;
  merge_context.progressive_merge_num_ = 0;
  const int64_t tables_count = merge_context.tables_handle_.get_count();
  merge_context.scn_range_.start_scn_ = merge_context.tables_handle_.get_table(0)->get_start_scn();
  merge_context.scn_range_.end_scn_ = merge_context.tables_handle_.get_table(tables_count - 1)->get_end_scn();
  merge_context.merge_scn_ = merge_context.scn_range_.end_scn_;

  ASSERT_EQ(OB_SUCCESS, merge_context.init_merge_info());
  ASSERT_EQ(OB_SUCCESS, merge_context.merge_info_.prepare_index_builder(index_desc_));
}

void TestMultiVersionMerge::build_sstable(
    ObTabletMergeCtx &ctx,
    ObSSTable *&merged_sstable)
{
  ASSERT_EQ(OB_SUCCESS, ctx.merge_info_.create_sstable(ctx));
  merged_sstable = &ctx.merged_sstable_;
}

TEST_F(TestMultiVersionMerge, rowkey_cross_two_macro_and_second_macro_is_filtered)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "0        var1  -8       0        NOP      1    T_DML_UPDATE  EXIST   LF\n"
      "1        var1  -8       0        2        2    T_DML_INSERT  EXIST   CLF\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "2        var1  -8       MIN      3       2     T_DML_UPDATE EXIST   SCF\n"
      "2        var1  -8       0        3       NOP   T_DML_UPDATE EXIST   N\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "2        var1  -6       0        2       2     T_DML_INSERT EXIST   CL\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_one_macro(&micro_data[2], 1);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  dml          flag    multi_version_row_flag\n"
      "0        var1  -10       0        NOP     10    T_DML_UPDATE EXIST   LF\n"
      "2        var1  -10       0        NOP     12    T_DML_UPDATE EXIST   LF\n"
      "3        var1  -10       0        NOP     13    T_DML_UPDATE EXIST   LF\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "0        var1  -10      MIN      NOP      10     EXIST   SF\n"
      "0        var1  -10      0        NOP      10     EXIST   N\n"
      "0        var1  -8       0        NOP      1      EXIST   L\n"
      "1        var1  -8       0        2        2      EXIST   CLF\n"
      "2        var1  -10      MIN      3        12     EXIST   SCF\n"
      "2        var1  -10      0        NOP      12     EXIST   N\n"
      "2        var1  -8       0        3        NOP    EXIST   N\n"
      "2        var1  -6       0        2        2      EXIST   CL\n"
      "3        var1  -10      0        NOP      13     EXIST   LF\n";

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
  ASSERT_TRUE(res_iter.equals(sstable_iter, true/*cmp multi version row flag*/));
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, rowkey_cross_three_macro_inc_merge)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[4];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint  dml          flag    multi_version_row_flag\n"
      "0        var1  -8       0        NOP      1     T_DML_UPDATE EXIST   LF\n"
      "1        var1  -8       MIN      3        3     T_DML_UPDATE EXIST   SCF\n"
      "1        var1  -8       0        3       NOP    T_DML_UPDATE EXIST   N\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint  dml  flag    multi_version_row_flag\n"
      "1        var1  -7       0        NOP    3      T_DML_UPDATE EXIST   N\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  bigint dml flag    multi_version_row_flag\n"
      "1        var1  -6       0        NOP       2    T_DML_UPDATE  EXIST   N\n";

  micro_data[3] =
      "bigint   var   bigint   bigint   bigint  bigint dml flag    multi_version_row_flag\n"
      "1        var1  -5       0        2       NOP   T_DML_UPDATE EXIST   L\n"
      "2        var1  -5       0        2       2     T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_one_macro(&micro_data[2], 1);
  prepare_one_macro(&micro_data[3], 1);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "0        var1  -10       0        NOP     10   T_DML_UPDATE  EXIST   LF\n"
      "2        var1  -10       0        NOP     12   T_DML_UPDATE  EXIST   LF\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "0        var1  -10      MIN      NOP      10     EXIST   SF\n"
      "0        var1  -10      0        NOP      10     EXIST   N\n"
      "0        var1  -8       0        NOP      1      EXIST   L\n"
      "1        var1  -8      MIN       3        3      EXIST   SCF\n"
      "1        var1  -8       0        3        NOP    EXIST   N\n"
      "1        var1  -7       0        NOP      3      EXIST   N\n"
      "1        var1  -6       0        NOP      2      EXIST   N\n"
      "1        var1  -5       0        2        NOP    EXIST   L\n"
      "2        var1  -10      MIN      2         12     EXIST   SCF\n"
      "2        var1  -10      0        NOP      12     EXIST   N\n"
      "2        var1  -5       0        2        2      EXIST   CL\n";

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
  ASSERT_TRUE(res_iter.equals(sstable_iter, true/*cmp multi version row flag*/));
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, uncommit_rowkey_committed_in_minor)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint  dml  flag    multi_version_row_flag\n"
      "0        var1  -8       0        NOP      1     T_DML_UPDATE EXIST   LF\n"
      "1        var1  -9       0        10       NOP   T_DML_UPDATE  EXIST   F\n"
      "1        var1  -8       MIN      3         3    T_DML_UPDATE  EXIST   SC\n"
      "1        var1  -8       0        3        NOP   T_DML_UPDATE  EXIST   N\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  bigint dml flag    multi_version_row_flag\n"
      "1        var1  -5       0        NOP     3     T_DML_UPDATE EXIST   L\n"
      "2        var1  -5       0        2       2     T_DML_INSERT EXIST   CLF\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint dml flag    multi_version_row_flag\n"
      "0        var1  -10       0        NOP     10   T_DML_UPDATE  EXIST   LF\n"
      "2        var1  -10       0        NOP     12   T_DML_UPDATE  EXIST   LF\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "0        var1  -10      MIN      NOP      10     EXIST   SF\n"
      "0        var1  -10      0        NOP      10     EXIST   N\n"
      "0        var1  -8       0        NOP      1      EXIST   L\n"
      "1        var1  -9       MIN      10       3      EXIST   SCF\n"
      "1        var1  -9       0        10       NOP    EXIST   N\n"
      "1        var1  -8       0        3        NOP    EXIST   N\n"
      "1        var1  -5       0        NOP      3      EXIST   L\n"
      "2        var1  -10      MIN      2        12     EXIST   SCF\n"
      "2        var1  -10      0        NOP      12     EXIST   N\n"
      "2        var1  -5       0        2        2      EXIST   CL\n";

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
  ASSERT_TRUE(res_iter.equals(sstable_iter, true/*cmp multi version row flag*/));
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, uncommit_rowkey_in_one_macro_committed_is_last)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint   flag    multi_version_row_flag\n"
      "0        var1  -8       0        NOP      1      EXIST   LF\n"
      "1        var1  -9       0        10       NOP     EXIST  LF\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag\n"
      "2        var1  -5       0        2       2      EXIST   CLF\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "0        var1  -10       0        NOP     10     EXIST   LF\n"
      "2        var1  -10       0        NOP     12     EXIST   LF\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "0        var1  -10      MIN      NOP      10     EXIST   SF\n"
      "0        var1  -10      0        NOP      10     EXIST   N\n"
      "0        var1  -8       0        NOP      1      EXIST   L\n"
      "1        var1  -9       0        10       NOP    EXIST   LF\n"
      "2        var1  -10      MIN      2        12     EXIST   SCF\n"
      "2        var1  -10      0        NOP      12     EXIST   N\n"
      "2        var1  -5       0        2        2      EXIST   CL\n";

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
  ASSERT_TRUE(res_iter.equals(sstable_iter, true/*cmp multi version row flag*/));
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, uncommit_rowkey_in_one_macro_committed_following_last)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint   flag    multi_version_row_flag\n"
      "0        var1  -8       0        NOP      1      EXIST   LF\n"
      "1        var1  -9       0        10       NOP     EXIST   F\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag\n"
      "1        var1  -5       0        NOP     3      EXIST   L\n"
      "2        var1  -5       0        2       2      EXIST   CLF\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "0        var1  -10       0        NOP     10     EXIST   LF\n"
      "2        var1  -10       0        NOP     12     EXIST   LF\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "0        var1  -10      -9223372036854775807  NOP      10     EXIST   SF\n"
      "0        var1  -10      0        NOP      10     EXIST   N\n"
      "0        var1  -8       0        NOP      1      EXIST   L\n"
      "1        var1  -9      -9223372036854775807  10 3 EXIST  SCF\n"
      "1        var1  -9       0        10       NOP    EXIST   N\n"
      "1        var1  -5       0        NOP      3      EXIST   L\n"
      "2        var1  -10      -9223372036854775807  2 12     EXIST   SCF\n"
      "2        var1  -10      0        NOP      12     EXIST   N\n"
      "2        var1  -5       0        2        2      EXIST   CL\n";

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
  ASSERT_TRUE(res_iter.equals(sstable_iter, true/*cmp multi version row flag*/));
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, uncommit_rowkey_in_one_macro_committed_following_shadow)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint   flag    multi_version_row_flag\n"
      "0        var1  -8       0        NOP      1      EXIST    LF\n"
      "1        var1  -9       0        10       NOP     EXIST   F\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag\n"
      "1        var1  -8       -9223372036854775807  3  3      EXIST   SC\n"
      "1        var1  -8       0        3       NOP     EXIST   N\n"
      "1        var1  -5       0        NOP     3      EXIST    L\n"
      "2        var1  -5       0        2       2      EXIST   CLF\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "0        var1  -10       0        NOP     10     EXIST   LF\n"
      "2        var1  -10       0        NOP     12     EXIST   LF\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "0        var1  -10      -9223372036854775807  NOP      10     EXIST   SF\n"
      "0        var1  -10      0        NOP      10     EXIST   N\n"
      "0        var1  -8       0        NOP      1      EXIST   L\n"
      "1        var1  -9      -9223372036854775807  10 3 EXIST   SCF\n"
      "1        var1  -9       0        10       NOP    EXIST   N\n"
      "1        var1  -8       0        3        NOP    EXIST   N\n"
      "1        var1  -5       0        NOP      3      EXIST   L\n"
      "2        var1  -10      -9223372036854775807  2 12     EXIST   SCF\n"
      "2        var1  -10      0        NOP      12     EXIST   N\n"
      "2        var1  -5       0        2        2      EXIST   CL\n";

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
  ASSERT_TRUE(res_iter.equals(sstable_iter, true/*cmp multi version row flag*/));
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, rowkey_cross_three_macro_full_merge)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[4];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint   flag    multi_version_row_flag\n"
      "0        var1  -8       0        2       NOP      EXIST   LF\n"
      "1        var1  -8      MIN       2  5    EXIST   SCF\n"
      "1        var1  -8       0        NOP     5        EXIST   N\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag\n"
      "1        var1  -7       0        NOP     4       EXIST   N\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag\n"
      "1        var1  -6       0        NOP     3       EXIST   N\n";

  micro_data[3] =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag\n"
      "1        var1  -5       0        2       NOP    EXIST    L\n"
      "2        var1  -5       0        2       NOP    EXIST    LF\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_one_macro(&micro_data[2], 1);
  prepare_one_macro(&micro_data[3], 1);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "0        var1  -10       0        NOP     10     EXIST   LF\n"
      "2        var1  -10       0        NOP     12     EXIST   LF\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, true/*is_full_merge*/, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "0        var1  -10      MIN       2       10     EXIST   SCF\n"
      "0        var1  -10      0        NOP      10     EXIST   N\n"
      "0        var1  -8       0        2        NOP    EXIST   L\n"
      "1        var1  -8       MIN      2        5      EXIST   SCF\n"
      "1        var1  -8       0        NOP      5      EXIST   N\n"
      "1        var1  -7       0        NOP      4      EXIST   N\n"
      "1        var1  -6       0        NOP      3      EXIST   N\n"
      "1        var1  -5       0        2        NOP    EXIST   L\n"
      "2        var1  -10      MIN      2        12     EXIST   SCF\n"
      "2        var1  -10      0        NOP      12     EXIST   N\n"
      "2        var1  -5       0        2        NOP    EXIST   L\n";

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
  ASSERT_TRUE(res_iter.equals(sstable_iter, true/*cmp multi version row flag*/));
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, test_merge_with_multi_trans)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint   flag    multi_version_row_flag\n"
      "0        var0   -9      -9        7       12      EXIST   CLF\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag  trans_id\n"
      "1        var1   MIN    -12     NOP     NOP     EXIST   FU  trans_id_4\n"
      "1        var1   -22    MIN     7       6       EXIST   SC  trans_id_0\n"
      "1        var1   -22    -10     7       6       EXIST   C   trans_id_0\n";

  micro_data[2] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1   -4    -1      NOP     9       EXIST   L   trans_id_0\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1, INT64_MAX, true);
  prepare_one_macro(&micro_data[2], 1, INT64_MAX, true);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "0        var0  MIN     -11      9        NOP     EXIST   ULF  trans_id_1\n"
      "1        var1  MIN     -16      8        NOP     EXIST   ULF  trans_id_4\n"
      "2        var2  MIN     -15      12       NOP     EXIST   FU  trans_id_1\n"
      "2        var2  -4       0       NOP      7       EXIST   L   trans_id_0\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1, INT64_MAX, true);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "2        var2  MIN       -25     18       NOP     EXIST   ULF   trans_id_1\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  prepare_data_end(handle3);
  merge_context.tables_handle_.add_table(handle3);
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

  for (int64_t i = 1; i <= 4; i++) {
    ObTxData *tx_data = new ObTxData();
    transaction::ObTransID tx_id = i;

    // fill in data
    tx_data->tx_id_ = tx_id;
    if (i < 4) {
      tx_data->commit_version_.convert_for_tx(i * 10 + i);
      tx_data->start_scn_.convert_for_tx(i);
      tx_data->end_scn_ = tx_data->commit_version_;
      tx_data->state_ = ObTxData::COMMIT;
    } else {
      tx_data->commit_version_.convert_for_tx(INT64_MAX);
      tx_data->start_scn_.convert_for_tx(i);
      tx_data->end_scn_.convert_for_tx(30);
      tx_data->state_ = ObTxData::RUNNING;
      transaction::ObUndoAction undo_action(ObTxSEQ(2, 0),ObTxSEQ(1, 0));
      tx_data->add_undo_action(tx_table, undo_action);
    }

    ASSERT_EQ(OB_SUCCESS, tx_table->insert(tx_data));
    delete tx_data;
  }

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "0        var0  -11      MIN    9        12      EXIST   SCF  trans_id_0\n"
      "0        var0  -11      0      9        NOP     EXIST   N  trans_id_0\n"
      "0        var0  -9       0      7        12      EXIST   CL trans_id_0\n"
      "1        var1  MIN     -16     8        NOP     EXIST   FU trans_id_4\n"
      "1        var1  MIN     -12     NOP      NOP     EXIST   U  trans_id_4\n"
      "1        var1  -22      MIN    7        6       EXIST   SC trans_id_0\n"
      "1        var1  -22      0      7        6       EXIST   C trans_id_0\n"
      "1        var1  -4       0      NOP      9       EXIST   L trans_id_0\n"
      "2        var2  -11     MIN    18       7       EXIST   SCF trans_id_0\n"
      "2        var2  -11      0      18       NOP       EXIST   N trans_id_0\n"
      "2        var2  -4       0      NOP      7       EXIST   L trans_id_0\n";

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
  ASSERT_TRUE(res_iter.equals(sstable_iter, true));
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  handle3.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, test_merge_with_multi_trans_can_compact)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[4];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint   flag    multi_version_row_flag\n"
      "0        var0   -9      -9        7       12      EXIST   CLF\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag  trans_id\n"
      "1        var1   -4    -14        1      1       EXIST    CLF  trans_id_0\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag  trans_id\n"
      "2        var2   MIN    -14       NOP     99      EXIST    FU  trans_id_4\n"
      "2        var2   -20    MIN       70      88      EXIST    SC  trans_id_0\n";

  micro_data[3] =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag  trans_id\n"
      "2        var2   -20     -13     NOP     88      EXIST   N  trans_id_0\n"
      "2        var2   -10     -10     70      NOP     EXIST   L  trans_id_0\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_one_macro(&micro_data[2], 1, INT64_MAX, true);
  prepare_one_macro(&micro_data[3], 1);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[5];
  micro_data2[0] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "0        var0  -9     -11       9        NOP     EXIST   LF   trans_id_0\n"
      "1        var1  MIN    -89       NOP      9       EXIST   FU   trans_id_3\n";

  micro_data2[1] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1  MIN    -80       NOP      8       EXIST   U   trans_id_3\n";

  micro_data2[2] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1  MIN    -70       7      NOP       EXIST   U   trans_id_3\n";

  micro_data2[3] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1  MIN    -30       NOP      5       EXIST   U   trans_id_3\n";

  micro_data2[4] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1  MIN    -10       NOP      3       EXIST   U   trans_id_3\n"
      "1        var1  -5     -10       2        2       EXIST   CL  trans_id_0\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(60);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1, INT64_MAX, true);
  prepare_one_macro(&micro_data2[1], 1, INT64_MAX, true);
  prepare_one_macro(&micro_data2[2], 1, INT64_MAX, true);
  prepare_one_macro(&micro_data2[3], 1, INT64_MAX, true);
  prepare_one_macro(&micro_data2[4], 1, INT64_MAX, true);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  ls_handle.get_ls()->get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  for (int64_t i = 1; i <= 5; i++) {
    ObTxData *tx_data = new ObTxData();
    transaction::ObTransID tx_id = i;

    // fill in data
    tx_data->tx_id_ = tx_id;
    if (i < 5) {
      tx_data->commit_version_.convert_for_tx(i * 10 + i);
      tx_data->start_scn_.convert_for_tx(i);
      tx_data->end_scn_ = tx_data->commit_version_;
      tx_data->state_ = ObTxData::COMMIT;
    } else {
      tx_data->commit_version_.convert_for_tx(INT64_MAX);
      tx_data->start_scn_.convert_for_tx(i);
      tx_data->end_scn_.convert_for_tx(50);
      tx_data->state_ = ObTxData::RUNNING;
      transaction::ObUndoAction undo_action(ObTxSEQ(2, 0),ObTxSEQ(1,0));
      tx_data->add_undo_action(tx_table, undo_action);
    }

    ASSERT_EQ(OB_SUCCESS, tx_table->insert(tx_data));
    delete tx_data;
  }

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "0        var0  -9       0        9        12      EXIST   CLF trans_id_0\n"
      "1        var1  -33     MIN       7        9        EXIST  SCF  trans_id_0\n"
      "1        var1  -33     0         7        9        EXIST  C  trans_id_0\n"
      "1        var1  -5      0         2        2        EXIST  C  trans_id_0\n"
      "1        var1  -4      0         1        1        EXIST  CL  trans_id_0\n"
      "2        var2  -44     MIN       70       99       EXIST   SCF trans_id_0\n"
      "2        var2  -44     0         NOP      99       EXIST   N trans_id_0\n"
      "2        var2   -20    0         NOP      88      EXIST   N  trans_id_0\n"
      "2        var2   -10    0         70       NOP     EXIST   L  trans_id_0\n";

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
  ASSERT_TRUE(res_iter.equals(sstable_iter, true));
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, test_merge_with_multi_trans_can_not_compact)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[5];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint   flag    multi_version_row_flag\n"
      "0        var0   -9      -9        7       12      EXIST   CLF\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag  trans_id\n"
      "1        var1   -44    MIN     7      59      EXIST    SCF  trans_id_0\n"
      "1        var1   -44    -14     NOP     59      EXIST    N  trans_id_0\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag  trans_id\n"
      "1        var1   -33     -13     NOP     28      EXIST   N  trans_id_0\n";

  micro_data[3] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1   -22     -12      NOP     71      EXIST  N  trans_id_0\n";

  micro_data[4] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1   -11    -7      7       2       EXIST   C   trans_id_0\n"
      "1        var1   -4      0      NOP     9       EXIST   L   trans_id_0\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1, INT64_MAX, true);
  prepare_one_macro(&micro_data[2], 1, INT64_MAX, true);
  prepare_one_macro(&micro_data[3], 1, INT64_MAX, true);
  prepare_one_macro(&micro_data[4], 1, INT64_MAX, true);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "0        var0  -9     -11       9        NOP     EXIST   LF   trans_id_0\n"
      "1        var1  MIN    -89       NOP      9       EXIST   FU   trans_id_5\n"
      "1        var1  -44    -17       NOP      100      EXIST   L    trans_id_0\n"
      "2        var2  MIN     -15      12       NOP     EXIST   FU  trans_id_1\n"
      "2        var2  -4       0       NOP      7       EXIST   L   trans_id_0\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1, INT64_MAX, true);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "2        var2  MIN       -25     18       NOP     EXIST   ULF   trans_id_1\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(100);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  prepare_data_end(handle3);
  merge_context.tables_handle_.add_table(handle3);
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

  for (int64_t i = 1; i <= 5; i++) {
    ObTxData *tx_data = new ObTxData();
    transaction::ObTransID tx_id = i;

    // fill in data
    tx_data->tx_id_ = tx_id;
    if (i < 5) {
      tx_data->commit_version_.convert_for_tx(i * 10 + i);
      tx_data->start_scn_.convert_for_tx(i);
      tx_data->end_scn_ = tx_data->commit_version_;
      tx_data->state_ = ObTxData::COMMIT;
    } else {
      tx_data->commit_version_.convert_for_tx(INT64_MAX);
      tx_data->start_scn_.convert_for_tx(i);
      tx_data->end_scn_.convert_for_tx(50);
      tx_data->state_ = ObTxData::RUNNING;
      transaction::ObUndoAction undo_action(ObTxSEQ(2, 0),ObTxSEQ(1, 0));
      tx_data->add_undo_action(tx_table, undo_action);
;
    }

    ASSERT_EQ(OB_SUCCESS, tx_table->insert(tx_data));
    delete tx_data;
  }

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  STORAGE_LOG(WARN, "full_read_info", K(full_read_info_));
  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "0        var0  -9       0      9        12      EXIST   CLF trans_id_0\n"
      "1        var1  MIN     -89     NOP      9       EXIST   FU trans_id_5\n"
      "1        var1  -44     MIN     7        100      EXIST  SC  trans_id_0\n"
      "1        var1  -44     0       NOP      100      EXIST   N  trans_id_0\n"
      "1        var1  -33     0       NOP      28      EXIST   N  trans_id_0\n"
      "1        var1  -22     0       NOP      71      EXIST   N  trans_id_0\n"
      "1        var1  -11     0       7        2       EXIST   C  trans_id_0\n"
      "1        var1  -4      0       NOP      9       EXIST   L  trans_id_0\n"
      "2        var2  -11     MIN    18       7       EXIST   SCF trans_id_0\n"
      "2        var2  -11      0      18       NOP       EXIST   N trans_id_0\n"
      "2        var2  -4       0      NOP      7       EXIST   L trans_id_0\n";

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
  ASSERT_TRUE(res_iter.equals(sstable_iter, true));
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  handle3.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, test_merge_with_macro_reused_with_shadow)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint   flag    multi_version_row_flag\n"
      "0        var0   -9      -9        7       12      EXIST   CLF\n"
      "1        var1   -44    MIN        7       59      EXIST    SCF \n"
      "1        var1   -44    -14        NOP     59      EXIST    N \n"
      "1        var1   -33    -13        NOP     28      EXIST    N\n";

  micro_data[1] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag \n"
      "1        var1   -11    -7      7       2       EXIST   C \n"
      "1        var1   -4      0      NOP     9       EXIST   L \n"
      "2        var2   -4      0      7       7       EXIST   CL \n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag \n"
      "2        var2  -30      MIN    NOP     20      EXIST   SF\n"
      "2        var2  -30      -10    NOP     20      EXIST   N \n"
      "2        var2  -20      -10    NOP     10      EXIST   L \n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag \n"
      "0        var0   -9      0        7       12      EXIST   CLF\n"
      "1        var1   -44    MIN       7       59      EXIST    SCF \n"
      "1        var1   -44     0        NOP     59      EXIST    N \n"
      "1        var1   -33     0        NOP     28      EXIST    N\n"
      "1        var1   -11     0        7       2       EXIST   C \n"
      "1        var1   -4      0        NOP     9       EXIST   L \n"
      "2        var2  -30      MIN      7       20      EXIST   SCF\n"
      "2        var2  -30      0        NOP     20      EXIST   N \n"
      "2        var2  -20      0        NOP     10      EXIST   N \n"
      "2        var2   -4      0        7       7       EXIST   CL\n";

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
  ASSERT_TRUE(res_iter.equals(sstable_iter, true));
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, test_merge_with_macro_reused_without_shadow)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint   flag    multi_version_row_flag\n"
      "0        var0   -9      -9        7       12      EXIST   CLF\n"
      "1        var1   -44    MIN        7       59      EXIST    SCF \n"
      "1        var1   -44    -14        NOP     59      EXIST    N \n"
      "1        var1   -33    -13         7      28      EXIST    CL\n";

  micro_data[1] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "2        var2   MIN      -10     20       NOP       EXIST   FU trans_id_2\n"
      "2        var2   -20      MIN     9        9         EXIST   SC trans_id_0\n"
      "2        var2   -20      -10     9        NOP       EXIST   N  trans_id_0\n"
      "2        var2   -10      -5      2        9         EXIST   CL trans_id_0\n"
      "3        var3   -10      0       3        3         EXIST   CLF trans_id_0\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1, INT64_MAX, true);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag \n"
      "3        var3   -30      0       30        NOP  EXIST   LF \n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  ls_handle.get_ls()->get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  for (int64_t i = 1; i <= 4; i++) {
    ObTxData *tx_data = new ObTxData();
    transaction::ObTransID tx_id = i;

    // fill in data
    tx_data->tx_id_ = tx_id;
    tx_data->commit_version_.convert_for_tx(i * 10 + i);
    tx_data->start_scn_.convert_for_tx(i);
    tx_data->end_scn_ = tx_data->commit_version_;
    tx_data->state_ = ObTxData::COMMIT;

    ASSERT_EQ(OB_SUCCESS, tx_table->insert(tx_data));
    delete tx_data;
  }

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag \n"
      "0        var0   -9       0        7       12      EXIST   CLF\n"
      "1        var1   -44      MIN      7       59      EXIST    SCF \n"
      "1        var1   -44      0        NOP     59      EXIST    N \n"
      "1        var1   -33      0        7      28      EXIST    CL\n"
      "2        var2   -22      MIN      20       9         EXIST   SCF\n"
      "2        var2   -22      0        20       NOP       EXIST   N \n"
      "2        var2   -20      0        9        NOP       EXIST   N  \n"
      "2        var2   -10      0        2        9         EXIST   CL \n"
      "3        var3   -30      MIN      30       3         EXIST   SCF \n"
      "3        var3   -30      0        30       NOP       EXIST   N \n"
      "3        var3   -10      0        3        3         EXIST   CL \n";

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
  ASSERT_TRUE(res_iter.equals(sstable_iter, true));
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, test_merge_with_greater_multi_version)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint   flag    multi_version_row_flag\n"
      "0        var0   -9      MIN        7       12      EXIST   SCF\n"
      "0        var0   -9      -9         NOP     12      EXIST   N\n"
      "0        var0   -8      -9         7       7       EXIST   CL\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint   flag    multi_version_row_flag\n"
      "0        var0   -20      MIN        15     15      EXIST   SCF\n"
      "0        var0   -20      -9         NOP     15      EXIST   N\n"
      "0        var0   -17      -9         15       NOP    EXIST   L\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag \n"
      "0        var0   -20      MIN        15     15      EXIST   SCF\n"
      "0        var0   -20      0          15     15      EXIST   C\n"
      "0        var0   -9       0          7      12      EXIST   CL\n";

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
  ASSERT_TRUE(res_iter.equals(sstable_iter, true));
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, test_merge_with_greater_multi_version_and_uncommit)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint   flag    multi_version_row_flag trans_id\n"
      "0        var0   MIN      -20       NOP     30      EXIST   FU      trans_id_1\n"
      "0        var0   -9      MIN        7       12      EXIST   SC      trans_id_0\n"
      "0        var0   -9      -9         NOP     12      EXIST   N       trans_id_0\n"
      "0        var0   -8      -9         7       7       EXIST   CL      trans_id_0\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1, INT64_MAX, true);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint   flag    multi_version_row_flag\n"
      "0        var0   -20      MIN        NOP    15      EXIST   SCF\n"
      "0        var0   -20      -9         NOP    15      EXIST   N\n"
      "0        var0   -17      -9         NOP    13      EXIST   N\n"
      "0        var0   -15      -9         NOP    11      EXIST   L\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.base_version_ = 1;

  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  ls_handle.get_ls()->get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  for (int64_t i = 1; i <= 4; i++) {
    ObTxData *tx_data = new ObTxData();
    transaction::ObTransID tx_id = i;

    // fill in data
    tx_data->tx_id_ = tx_id;
    tx_data->commit_version_.convert_for_tx(i * 10 + i);
    tx_data->start_scn_.convert_for_tx(i);
    tx_data->end_scn_ = tx_data->commit_version_;
    tx_data->state_ = ObTxData::COMMIT;

    ASSERT_EQ(OB_SUCCESS, tx_table->insert(tx_data));
    delete tx_data;
  }

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag \n"
      "0        var0   -20      MIN        7     15      EXIST   SCF\n"
      "0        var0   -20      0          NOP   15      EXIST   N\n"
      "0        var0   -11      0          7     30      EXIST   CL\n";

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
  ASSERT_TRUE(res_iter.equals(sstable_iter, true));
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, test_merge_with_ghost_row)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1   MIN      -6      18     2      EXIST   FU    trans_id_1\n"
      "1        var1   MIN      -3      NOP    NOP    EXIST   U     trans_id_1\n"
      "1        var1   MIN      -2      NOP    19     EXIST   LU    trans_id_1\n"
      "2        var2   MIN      -9      18     0      EXIST   ULF   trans_id_2\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1, INT64_MAX, true);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1   MIN      -38      NOP    59      EXIST   ULF   trans_id_3\n"
      "2        var2   MIN      -38      NOP    59      EXIST   ULF   trans_id_2\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1, INT64_MAX, true);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1   MIN      -48      NOP    59      EXIST   ULF   trans_id_3\n"
      "2        var2   MIN      -71      18     1       EXIST   FU    trans_id_4\n"
      "2        var2   MAGIC    MAGIC    NOP    NOP     EXIST   LG    trans_id_0\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1, INT64_MAX, true);
  prepare_data_end(handle3);
  merge_context.tables_handle_.add_table(handle3);
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

  for (int64_t i = 1; i <= 4; i++) {
    ObTxData *tx_data = new ObTxData();
    transaction::ObTransID tx_id = i;

    // fill in data
    tx_data->tx_id_ = tx_id;
    if (i % 2 == 1) {
      tx_data->commit_version_.convert_for_tx(INT64_MAX);
      tx_data->start_scn_.convert_for_tx(i);
      tx_data->end_scn_.convert_for_tx(i * 10 + i);
      tx_data->state_ = ObTxData::ABORT;
    } else {
      tx_data->commit_version_.convert_for_tx(i * 10 + i);
      tx_data->start_scn_.convert_for_tx(i);
      tx_data->end_scn_ = tx_data->commit_version_;
      tx_data->state_ = ObTxData::COMMIT;
    }

    ASSERT_EQ(OB_SUCCESS, tx_table->insert(tx_data));
    delete tx_data;
  }

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag \n"
      "2        var2  -44       MIN    18       1       EXIST  SCF \n"
      "2        var2  -44       0      18       1       EXIST   C  \n"
      "2        var2  -22       0      18       59      EXIST   CL \n";

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
  ASSERT_TRUE(res_iter.equals(sstable_iter, true));
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, compare_dml_flag)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var1  -8       0        1        1    INSERT  CLF\n"
      "2        var2  -6       0        2        2    INSERT  CLF\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "0        var1  -10       0        NOP     10    UPDATE  LF\n"
      "2        var2  -12       0        NOP     NOP   DELETE  CLF\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag\n"
      "0        var1  -10      MIN      1      10      INSERT  SCF\n"
      "0        var1  -10      0        NOP    10      UPDATE  N\n"
      "0        var1  -8       0        1      1       INSERT  CL\n"
      "2        var2  -12      MIN       NOP     NOP   DELETE  SCF\n"
      "2        var2  -12       0        NOP     NOP   DELETE  C\n"
      "2        var2  -6       0        2        2    INSERT   CL\n";

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
  ASSERT_TRUE(res_iter.equals(sstable_iter, true/*cmp multi version row flag*/));
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, get_last_after_reuse)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -15       0        NOP      1    EXIST   LF\n"
      "1        var1  -15       MIN      12       2    EXIST  SCF\n"
      "1        var1  -15       0        12       NOP    EXIST  N\n"
      "1        var1  -10       0        2         2    EXIST  CL\n"
      "3        var3  -8        MIN      12      12   EXIST   SCF\n"
      "3        var3  -8        0       12      NOP   EXIST   N\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "3        var3  -6        0      2      12   EXIST   CL\n"
      "4        var4  -15       0      2        2    EXIST   CLF\n"
      "5        var5  -15       0      NOP      12   EXIST   LF\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 20;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "5        var5  -20      0        NOP    13    EXIST   LF\n";

  snapshot_version = 30;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "0        var0  -15       0        NOP      1    EXIST   LF\n"
      "1        var1  -15       MIN      12       2    EXIST  SCF\n"
      "1        var1  -15       0        12       NOP    EXIST  N\n"
      "1        var1  -10       0        2         2    EXIST  CL\n"
      "3        var3  -8        MIN      12      12   EXIST   SCF\n"
      "3        var3  -8        0       12      NOP   EXIST   N\n"
      "3        var3  -6        0      2      12   EXIST   CL\n"
      "4        var4  -15       0      2        2    EXIST   CLF\n"
      "5        var5  -20       MIN      NOP      13   EXIST   SF\n"
      "5        var5  -20       0      NOP      13   EXIST   N\n"
      "5        var5  -15       0      NOP      12   EXIST   L\n";

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
  ASSERT_TRUE(res_iter.equals(sstable_iter, true/*cmp multi version row flag*/));
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}
TEST_F(TestMultiVersionMerge, rowkey_cross_two_macro_with_commit_scn_less_multi_version_start)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "1        var1  -8       0        2        2    T_DML_INSERT  EXIST   CLF\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "2        var1  -8       MIN      3       2     T_DML_UPDATE EXIST   SCF\n"
      "2        var1  -8       0        3       NOP   T_DML_UPDATE EXIST   N\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "2        var1  -6       0        2       2     T_DML_INSERT EXIST   CL\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_one_macro(&micro_data[2], 1);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  dml          flag    multi_version_row_flag\n"
      "0        var1  -10       0        NOP     10    T_DML_UPDATE EXIST   LF\n"
      "3        var1  -10       0        NOP     13    T_DML_UPDATE EXIST   LF\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 10;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "0        var1  -10      0        NOP      10     EXIST   LF\n"
      "1        var1  -8       0        2        2      EXIST   CLF\n"
      "2        var1  -8       MIN      3       2       EXIST   SCF\n"
      "2        var1  -8       0        3        2      EXIST   C\n"
      "2        var1  -6       0        2       2       EXIST   CL\n"
      "3        var1  -10      0        NOP      13     EXIST   LF\n";

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
  ASSERT_TRUE(res_iter.equals(sstable_iter, true/*cmp multi version row flag*/));
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, rowkey_cross_macro_with_last_shadow_version_less_than_multi_version)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag trans_id\n"
      "1        var1  -8       0        2        2    EXIST   CLF   trans_id_0\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag trans_id\n"
      "2        var2  MIN       -11      NOP      9   EXIST   FU    trans_id_2\n"
      "2        var2  -8        MIN      3        2   EXIST   SC    trans_id_0\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag trans_id\n"
      "2        var2  -8       0        3       NOP   EXIST   N     trans_id_0\n"
      "2        var2  -6       0        2       2     EXIST   CL    trans_id_0\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_one_macro(&micro_data[2], 1);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "1        var1  -32       0        NOP     10    EXIST   LF\n"
      "3        var3  -40       0        NOP     13    EXIST   LF\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  ls_handle.get_ls()->get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  for (int64_t i = 1; i < 3; i++) {
    ObTxData *tx_data = new ObTxData();
    transaction::ObTransID tx_id = i;

    // fill in data
    tx_data->tx_id_ = tx_id;
    tx_data->commit_version_.convert_for_tx(i * 10 + i);
    tx_data->start_scn_.convert_for_tx(i);
    tx_data->end_scn_ = tx_data->commit_version_;
    tx_data->state_ = ObTxData::COMMIT;

    ASSERT_EQ(OB_SUCCESS, tx_table->insert(tx_data));
    delete tx_data;
  }

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 10;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "1        var1  -32       MIN        2        10      EXIST   SCF\n"
      "1        var1  -32       0        NOP     10    EXIST      \n"
      "1        var1  -8       0        2        2      EXIST   CL\n"
      "2        var2  -22      MIN      3       9       EXIST   SCF\n"
      "2        var2  -22      0      NOP       9       EXIST   \n"
      "2        var2  -8       0        3        2      EXIST   CL\n"
      "3        var3  -40      0        NOP      13     EXIST   LF\n";

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
  ASSERT_TRUE(res_iter.equals(sstable_iter, true/*cmp multi version row flag*/));
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, shadow_row_is_last_in_macro)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag \n"
      "1        var1  -8       0        2        2    EXIST   CLF   \n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag \n"
      "2        var2  -10        MIN      3      3   EXIST    SCF   \n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag \n"
      "2        var2  -10       0        3       NOP   EXIST   N     \n"
      "2        var2  -8        0        NOP     3     EXIST   N     \n"
      "2        var2  -6       0         2       2     EXIST   CL    \n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_one_macro(&micro_data[2], 1);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "1        var1  -32       0        NOP     10    EXIST   LF\n"
      "2        var2  -10        MIN      3      3   EXIST    SCF\n"
      "2        var2  -10       0        3       NOP   EXIST   N     \n"
      "2        var2  -8        0        NOP     3     EXIST   N     \n"
      "2        var2  -6       0         2       2     EXIST   CL    \n"
      "3        var3  -40       0        NOP     13    EXIST   LF\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "1        var1  -32       MIN        2     10      EXIST   SCF\n"
      "1        var1  -32       0        NOP     10    EXIST      N\n"
      "1        var1  -8       0        2        2      EXIST   CL\n"
      "2        var2  -10      MIN       3       3     EXIST      SCF\n"
      "2        var2  -10      0         3      NOP    EXIST      N\n"
      "2        var2  -8       0         NOP     3      EXIST    N\n"
      "2        var2  -6       0         2       2      EXIST   CL\n"
      "3        var3  -40      0        NOP      13     EXIST   LF\n";

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
  ASSERT_TRUE(res_iter.equals(sstable_iter, true/*cmp multi version row flag*/));
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, rowkey_cross_macro_without_open_next_macro)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag trans_id\n"
      "0        var2  -10       0        2       2     EXIST   CLF    trans_id_0\n"
      "2        var2  MIN       -11      NOP      9   EXIST   FU    trans_id_2\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag trans_id\n"
      "2        var2  -8       MIN      3       2     EXIST   SC    trans_id_0\n"
      "2        var2  -8       0        3       NOP   EXIST   N     trans_id_0\n"
      "2        var2  -6       0        2       2     EXIST   CL    trans_id_0\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag trans_id\n"
      "3        var2  -8       0        3       2     EXIST    CLF    trans_id_0\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_one_macro(&micro_data[2], 1);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "1        var1  -32       0        NOP     10    EXIST   LF\n"
      "2        var2  -40       0        NOP     3     EXIST   LF\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  ls_handle.get_ls()->get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  for (int64_t i = 1; i < 3; i++) {
    ObTxData *tx_data = new ObTxData();
    transaction::ObTransID tx_id = i;

    // fill in data
    tx_data->tx_id_ = tx_id;
    tx_data->commit_version_.convert_for_tx(INT64_MAX);
    tx_data->start_scn_.convert_for_tx(i);
    tx_data->end_scn_.convert_for_tx(100);
    tx_data->state_ = ObTxData::ABORT;
    ASSERT_EQ(OB_SUCCESS, tx_table->insert(tx_data));
    delete tx_data;
  }

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
    "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag trans_id\n"
    "0        var2  -10       0        2       2     EXIST   CLF    trans_id_0\n"
    "1        var1  -32      0        NOP     10    EXIST   LF trans_id_0\n"
    "2        var2  -40      MIN      3       3       EXIST   SCF  trans_id_0\n"
    "2        var2  -40      0        NOP     3     EXIST   N   trans_id_0\n"
    "2        var2  -8       0        3       NOP   EXIST   N   trans_id_0\n"
    "2        var2  -6       0        2       2       EXIST   CL  trans_id_0\n"
    "3        var2  -8       0        3       2     EXIST    CLF    trans_id_0\n";

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
  ASSERT_TRUE(res_iter.equals(sstable_iter, true/*cmp multi version row flag*/));
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, range_cross_macro)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag \n"
      "0        var0  -10       0        2       2     EXIST   CLF    \n"
      "2        var2  -20     MIN      15        15   EXIST    SCF    \n"
      "2        var2  -20     -11      NOP       15   EXIST     N      \n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag \n"
      "2        var2  -10      0         15       NOP     EXIST  N \n"
      "2        var2  -8       0         2        2       EXIST  CL \n"
      "3        var3  -20     MIN        25       25      EXIST  SCF \n"
      "3        var3  -20     0         25       NOP      EXIST  N  \n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag \n"
      "3        var3  -10       0        NOP      25     EXIST   N \n"
      "3        var3  -5        0        5        5      EXIST   CL \n"
      "5        var5  -5        0        7        7      EXIST   CLF \n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 2);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "1        var1  -32       0        NOP     10    EXIST   LF\n"
      "4        var4  -40       0        NOP     3     EXIST   LF\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  ObDatumRow row;
  row.init(allocator_, 5);
  row.storage_datums_[0].set_int(2);
  row.storage_datums_[1].set_max();
  row.storage_datums_[2].set_max();
  row.storage_datums_[3].set_max();
  row.count_ = 4;
  merge_context.parallel_merge_ctx_.range_array_.at(0).start_key_.assign(row.storage_datums_, 4);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
    "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag \n"
      "3        var3  -20     MIN        25       25    EXIST  SCF \n"
      "3        var3  -20     0         25       NOP    EXIST  N  \n"
      "3        var3  -10      0        NOP      25     EXIST   N \n"
      "3        var3  -5       0        5        5      EXIST   CL \n"
      "4        var4  -40      0        NOP     3       EXIST   LF \n"
      "5        var5  -5       0        7        7      EXIST   CLF \n";

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
  ASSERT_TRUE(res_iter.equals(sstable_iter, true/*cmp multi version row flag*/));
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, test_merge_base_iter_have_ghost_row)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1   MIN      -6      18     2      EXIST   FU    trans_id_1\n"
      "1        var1   MIN      -3      NOP    NOP    EXIST   U     trans_id_1\n"
      "1        var1   MIN      -2      NOP    19     EXIST   LU    trans_id_1\n"
      "2        var2   MIN      -9      18     0      EXIST   ULF   trans_id_2\n";
  micro_data[1] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "3        var0   MIN      -71      18     1       EXIST   ULF    trans_id_1\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1, INT64_MAX, true);
  prepare_one_macro(&micro_data[1], 1, INT64_MAX, true);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1   MIN      -38      NOP    59      EXIST   ULF   trans_id_3\n"
      "2        var2   MIN      -38      NOP    59      EXIST   ULF   trans_id_2\n"
      "3        var0  -15       0        NOP      1    EXIST    LF     trans_id_2\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1, INT64_MAX, true);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1   MIN      -48      NOP    59      EXIST   ULF   trans_id_3\n"
      "2        var2   MIN      -71      18     1       EXIST   FU    trans_id_4\n"
      "2        var2   MAGIC    MAGIC    NOP    NOP     EXIST   LG    trans_id_0\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1, INT64_MAX, true);
  prepare_data_end(handle3);
  merge_context.tables_handle_.add_table(handle3);
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

  for (int64_t i = 1; i <= 4; i++) {
    ObTxData *tx_data = new ObTxData();
    transaction::ObTransID tx_id = i;

    // fill in data
    tx_data->tx_id_ = tx_id;
    if (i % 2 == 1) {
      tx_data->commit_version_.convert_for_tx(INT64_MAX);
      tx_data->start_scn_.convert_for_tx(i);
      tx_data->end_scn_.convert_for_tx(i * 10 + i);
      tx_data->state_ = ObTxData::ABORT;
    } else {
      tx_data->commit_version_.convert_for_tx(i * 10 + i);
      tx_data->start_scn_.convert_for_tx(i);
      tx_data->end_scn_ = tx_data->commit_version_;
      tx_data->state_ = ObTxData::COMMIT;
    }


    ASSERT_EQ(OB_SUCCESS, tx_table->insert(tx_data));
    delete tx_data;
  }

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag \n"
      "2        var2  -44       MIN    18       1       EXIST  SCF \n"
      "2        var2  -44       0      18       1       EXIST   C  \n"
      "2        var2  -22       0      18       59      EXIST   CL \n"
      "3        var0  -15       0      NOP      1    EXIST    LF  \n";

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
  ASSERT_TRUE(res_iter.equals(sstable_iter, true));
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}


TEST_F(TestMultiVersionMerge, test_major_range_cross_macro)
{
  int ret = OB_SUCCESS;
  fake_freeze_info();
  ObPartitionMajorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag \n"
      "0        var0  -10       0        2       2     EXIST   CLF    \n"
      "1        var1  -30     -11      1       15    EXIST   CLF    \n"
      "2        var2  -20     -11      2       15    EXIST   CLF    \n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag \n"
      "3        var3  -20     0        25       25      EXIST  CLF \n"
      "4        var4  -20     0         25       8      EXIST  CLF  \n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag \n"
      "5        var5  -5        0        7        7      EXIST   CLF \n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 100;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 2);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "9        var9  -140       0        2     3     EXIST   LF\n";

  snapshot_version = 200;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 200;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
  ObDatumRow row;
  row.init(allocator_, 5);
  row.storage_datums_[0].set_int(1);
  merge_context.parallel_merge_ctx_.range_array_.at(0).start_key_.assign(row.storage_datums_, 1);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
    "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag \n"
      "2        var2  -20     0        2     15    EXIST   N \n"
      "3        var3  -20     0        25      25    EXIST   N \n"
      "4        var4  -20     0        25      8   EXIST   N \n"
      "5        var5  -5      0        7       7     EXIST   N \n"
      "9        var9  -140    0        2     3     EXIST   N \n";

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
  ASSERT_TRUE(res_iter.equals(sstable_iter, true/*cmp multi version row flag*/));
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_multi_version_sstable_merge.log*");
  OB_LOGGER.set_file_name("test_multi_version_sstable_merge.log");
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
