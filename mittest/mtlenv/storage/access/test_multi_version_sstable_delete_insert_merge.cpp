// owner: zhanghuidong.zhd
// owner group: storage

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
      if (TX_DATA_ARR.at(i).state_ == ObTxData::RUNNING) {
        SCN tmp_scn;
        tmp_scn.convert_from_ts(30);
        ObTxCCCtx tmp_ctx(ObTxState::PREPARE, tmp_scn);
        ret = fn(TX_DATA_ARR[i], &tmp_ctx);
      } else {
        ret = fn(TX_DATA_ARR[i]);
      }
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "check with tx data failed", KR(ret), K(read_tx_data_arg), K(TX_DATA_ARR.at(i)));
      }
      break;
    }
  }
  return ret;
}

void clear_tx_data()
{
  TX_DATA_ARR.reset();
};


class TestMultiVersionDIMerge : public TestMergeBasic
{
public:
  static const int64_t MAX_PARALLEL_DEGREE = 10;
  TestMultiVersionDIMerge();
  virtual ~TestMultiVersionDIMerge() {}

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
  void get_tx_table_guard(ObTxTableGuard &tx_table_guard);
public:
  ObStoreCtx store_ctx_;
  ObTabletMergeExecuteDag merge_dag_;
};

void TestMultiVersionDIMerge::SetUpTestCase()
{
  ObMultiVersionSSTableTest::SetUpTestCase();
  // mock sequence no
  ObClockGenerator::init();

  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  MERGE_SCHEDULER_PTR->resume_major_merge();

  // create tablet
  share::schema::ObTableSchema table_schema;
  uint64_t table_id = 12345;
  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, table_id));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema, allocator_));
}

void TestMultiVersionDIMerge::TearDownTestCase()
{
  clear_tx_data();
  ObMultiVersionSSTableTest::TearDownTestCase();
  // reset sequence no
  ObClockGenerator::destroy();
}

TestMultiVersionDIMerge::TestMultiVersionDIMerge()
  : TestMergeBasic("test_multi_version_delete_insert_merge")
{}

void TestMultiVersionDIMerge::SetUp()
{
  ObMultiVersionSSTableTest::SetUp();
}

void TestMultiVersionDIMerge::fake_freeze_info()
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

void TestMultiVersionDIMerge::TearDown()
{
  ObMultiVersionSSTableTest::TearDown();
  TRANS_LOG(INFO, "teardown success");
}

void TestMultiVersionDIMerge::prepare_query_param(const ObVersionRange &version_range)
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
  iter_param_.is_delete_insert_ = true;
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
  //query_flag.multi_version_minor_merge_ = true;
  ASSERT_EQ(OB_SUCCESS,
            context_.init(query_flag,
                          store_ctx_,
                          allocator_,
                          allocator_,
                          version_range));
  context_.limit_param_ = nullptr;
}

void TestMultiVersionDIMerge::prepare_merge_context(const ObMergeType &merge_type,
                                                  const bool is_full_merge,
                                                  const ObVersionRange &trans_version_range,
                                                  ObTabletMergeCtx &merge_context)
{
  TestMergeBasic::prepare_merge_context(merge_type, is_full_merge, trans_version_range, merge_context);
  merge_context.merge_dag_ = &merge_dag_;
  merge_context.static_param_.for_unittest_ = true;
  merge_context.static_param_.is_delete_insert_merge_ = true;
  merge_context.static_param_.data_version_ = DATA_VERSION_4_3_5_2;
  ASSERT_EQ(OB_SUCCESS, merge_context.cal_merge_param());
  ASSERT_EQ(OB_SUCCESS, merge_context.init_parallel_merge_ctx());
  ASSERT_EQ(OB_SUCCESS, merge_context.static_param_.init_static_info(merge_context.tablet_handle_));
  ASSERT_EQ(OB_SUCCESS, merge_context.init_static_desc());
  ASSERT_EQ(OB_SUCCESS, merge_context.init_read_info());
  ASSERT_EQ(OB_SUCCESS, merge_context.init_tablet_merge_info());
  ASSERT_EQ(OB_SUCCESS, merge_context.merge_info_.prepare_sstable_builder());
  ASSERT_EQ(OB_SUCCESS, merge_context.merge_info_.sstable_builder_.data_store_desc_.init(merge_context.static_desc_, table_merge_schema_));
  ASSERT_EQ(OB_SUCCESS, merge_context.merge_info_.prepare_index_builder());
}

void TestMultiVersionDIMerge::build_sstable(
    ObTabletMergeCtx &ctx,
    ObSSTable *&merged_sstable)
{
  bool tmp_bool = false; // placeholder
  ASSERT_EQ(OB_SUCCESS, ctx.merge_info_.create_sstable(ctx, ctx.merged_table_handle_, tmp_bool));
  ASSERT_EQ(OB_SUCCESS, ctx.merged_table_handle_.get_sstable(merged_sstable));
}

void TestMultiVersionDIMerge::get_tx_table_guard(ObTxTableGuard &tx_table_guard)
{
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tx_table_guard(tx_table_guard));
}

TEST_F(TestMultiVersionDIMerge, rowkey_cross_two_macro_and_second_macro_is_filtered)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);
  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -8       DI_VERSION   -1      1    INSERT    NORMAL    CF\n"
      "0        var0  -8       0            -1     -1    DELETE    NORMAL    CL\n"
      "1        var1  -8       0            2       2    INSERT    NORMAL    CLF\n";

  micro_data[1] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "2        var2  -8       MIN          3       2     INSERT    INSERT_DELETE   SCF\n"
      "2        var2  -8       DI_VERSION   3       2     INSERT    NORMAL   C\n"
      "2        var2  -8       0            3       2     DELETE    NORMAL   C\n";

  micro_data[2] =
      "bigint   var   bigint   bigint     bigint  bigint  flag    flag_type  multi_version_row_flag\n"
      "2        var2  -6       0            2       2     INSERT    NORMAL   CL\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_one_macro(&micro_data[2], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -10      DI_VERSION -1      10       INSERT    NORMAL   CF\n"
      "0        var0  -10      0          -1      1        DELETE    NORMAL   CL\n"
      "2        var2  -10      DI_VERSION 3       12       INSERT    NORMAL   CF\n"
      "2        var2  -10      0          3       2        DELETE    NORMAL   CL\n"
      "3        var3  -10      DI_VERSION -1      13       INSERT    NORMAL   CF\n"
      "3        var3  -10      0          -1      -1       DELETE    NORMAL   CL\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -10      MIN        -1      10       INSERT    INSERT_DELETE   SCF\n"
      "0        var0  -10      DI_VERSION -1      10       INSERT    NORMAL   C\n"
      "0        var0  -10      0          -1      1        DELETE    NORMAL   C\n"
      "0        var0  -8       DI_VERSION -1      1        INSERT    NORMAL   C\n"
      "0        var0  -8       0          -1     -1        DELETE    NORMAL   CL\n"
      "1        var1  -8       DI_VERSION 2       2        INSERT    NORMAL   CLF\n"
      "2        var2  -10      MIN        3        12      INSERT    INSERT_DELETE   SCF\n"
      "2        var2  -10      DI_VERSION 3       12       INSERT    NORMAL   C\n"
      "2        var2  -10      0          3       2        DELETE    NORMAL   C\n"
      "2        var2  -8       DI_VERSION 3       2        INSERT    NORMAL   C\n"
      "2        var2  -8       0          3       2        DELETE    NORMAL   C\n"
      "2        var2  -6       DI_VERSION 2       2        INSERT    NORMAL   CL\n"
      "3        var3  -10      DI_VERSION -1      13       INSERT    NORMAL   CF\n"
      "3        var3  -10      0          -1      -1       DELETE    NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, rowkey_cross_three_macro_inc_merge)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[4];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -8       DI_VERSION -1      1        INSERT    NORMAL   CF\n"
      "0        var0  -8       0          -1     -1        DELETE    NORMAL   CL\n"
      "1        var1  -8       MIN         3      3        INSERT    INSERT_DELETE   SCF\n"
      "1        var1  -8       DI_VERSION  3      3        INSERT    NORMAL   C\n"
      "1        var1  -8       0           2      3        DELETE    NORMAL   C\n";

  micro_data[1] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -7       DI_VERSION  2      3        INSERT    NORMAL   C\n"
      "1        var1  -7       0           2      2        DELETE    NORMAL   C\n";

  micro_data[2] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -6       DI_VERSION  2      2        INSERT    NORMAL   C\n"
      "1        var1  -6       0           2     -1        DELETE    NORMAL   C\n";

  micro_data[3] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -5       DI_VERSION  2     -1        INSERT    NORMAL   C\n"
      "1        var1  -5       0          -1     -1        DELETE    NORMAL   CL\n"
      "2        var2  -5       DI_VERSION  2      2        INSERT    NORMAL   CF\n"
      "2        var2  -5       0          -1     -1        DELETE    NORMAL   CL\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_one_macro(&micro_data[2], 1);
  prepare_one_macro(&micro_data[3], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -10      DI_VERSION -1      10       INSERT    NORMAL   CF\n"
      "0        var0  -10      0          -1      1        DELETE    NORMAL   CL\n"
      "2        var2  -10      DI_VERSION  2      12       INSERT    NORMAL   CF\n"
      "2        var2  -10      0           2      2        DELETE    NORMAL   CL\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -10      MIN        -1      10       INSERT    INSERT_DELETE   SCF\n"
      "0        var0  -10      DI_VERSION -1      10       INSERT    NORMAL   C\n"
      "0        var0  -10      0          -1      1        DELETE    NORMAL   C\n"
      "0        var0  -8       DI_VERSION -1      1        INSERT    NORMAL   C\n"
      "0        var0  -8       0          -1     -1        DELETE    NORMAL   CL\n"
      "1        var1  -8       MIN         3      3        INSERT    INSERT_DELETE   SCF\n"
      "1        var1  -8       DI_VERSION  3      3        INSERT    NORMAL   C\n"
      "1        var1  -8       0           2      3        DELETE    NORMAL   C\n"
      "1        var1  -7       DI_VERSION  2      3        INSERT    NORMAL   C\n"
      "1        var1  -7       0           2      2        DELETE    NORMAL   C\n"
      "1        var1  -6       DI_VERSION  2      2        INSERT    NORMAL   C\n"
      "1        var1  -6       0           2     -1        DELETE    NORMAL   C\n"
      "1        var1  -5       DI_VERSION  2     -1        INSERT    NORMAL   C\n"
      "1        var1  -5       0          -1     -1        DELETE    NORMAL   CL\n"
      "2        var2  -10      MIN         2      12       INSERT    INSERT_DELETE   SCF\n"
      "2        var2  -10      DI_VERSION  2      12       INSERT    NORMAL   C\n"
      "2        var2  -10      0           2      2        DELETE    NORMAL   C\n"
      "2        var2  -5       DI_VERSION  2      2        INSERT    NORMAL   C\n"
      "2        var2  -5       0          -1     -1        DELETE    NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, uncommit_rowkey_committed_in_minor)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag trans_id\n"
      "0        var0  -8       DI_VERSION -1       1       INSERT    NORMAL   CF  trans_id_0\n"
      "0        var0  -8       0          -1      -1       DELETE    NORMAL   CL  trans_id_0\n"
      "1        var1  MIN     -99          10      3       INSERT    NORMAL   UCF trans_id_1\n"
      "1        var1  MIN     -98          3       3       DELETE    NORMAL   UC  trans_id_1\n"
      "1        var1  -8       MIN         3       3       INSERT    INSERT_DELETE   SC  trans_id_0\n"
      "1        var1  -8       DI_VERSION  3       3       INSERT    NORMAL   C   trans_id_0\n"
      "1        var1  -8       0          -1       3       DELETE    NORMAL   C   trans_id_0\n";

  micro_data[1] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -5       DI_VERSION -1       3       INSERT    NORMAL   C\n"
      "1        var1  -5       0          -1      -1       DELETE    NORMAL   CL\n"
      "2        var2  -5       DI_VERSION  2       3       INSERT    NORMAL   CF\n"
      "2        var2  -5       0          -1      -1       DELETE    NORMAL   CL\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -10       DI_VERSION  -1     10       INSERT    NORMAL   CF\n"
      "0        var0  -10       0           -1     1        DELETE    NORMAL   CL\n"
      "2        var2  -10       DI_VERSION   2     12       INSERT    NORMAL   CF\n"
      "2        var2  -10       0            2     3        DELETE    NORMAL   CL\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  for (int64_t i = 1; i <= 1; i++) {
    ObTxData *tx_data = new ObTxData();
    transaction::ObTransID tx_id = i;

    // fill in data
    tx_data->tx_id_ = tx_id;
    tx_data->commit_version_.convert_for_tx(i * 10);
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
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -10       MIN         -1       10    INSERT    INSERT_DELETE   SCF\n"
      "0        var0  -10       DI_VERSION  -1       10    INSERT    NORMAL   C\n"
      "0        var0  -10       0           -1       1     DELETE    NORMAL   C\n"
      "0        var0  -8        DI_VERSION  -1       1     INSERT    NORMAL   C\n"
      "0        var0  -8        0           -1      -1     DELETE    NORMAL   CL\n"
      "1        var1  -10       MIN          10      3     INSERT    INSERT_DELETE   SCF\n"
      "1        var1  -10       DI_VERSION   10      3     INSERT    NORMAL   C\n"
      "1        var1  -10       0            3       3     DELETE    NORMAL   C\n"
      "1        var1  -8        DI_VERSION   3       3     INSERT    NORMAL   C\n"
      "1        var1  -8        0           -1       3     DELETE    NORMAL   C\n"
      "1        var1  -5        DI_VERSION  -1       3     INSERT    NORMAL   C\n"
      "1        var1  -5        0           -1      -1     DELETE    NORMAL   CL\n"
      "2        var2  -10       MIN          2       12    INSERT    INSERT_DELETE   SCF\n"
      "2        var2  -10       DI_VERSION   2       12    INSERT    NORMAL   C\n"
      "2        var2  -10       0            2       3     DELETE    NORMAL   C\n"
      "2        var2  -5        DI_VERSION   2       3     INSERT    NORMAL   C\n"
      "2        var2  -5        0           -1      -1     DELETE    NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, uncommit_rowkey_in_one_macro_committed_is_last)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -8       DI_VERSION  -1       1     INSERT    NORMAL   CF\n"
      "0        var0  -8       0           -1      -1     DELETE    NORMAL   CL\n"
      "1        var1  -9       DI_VERSION   10     -1     INSERT    NORMAL   CF\n"
      "1        var1  -9       0           -1      -1     DELETE    NORMAL   CL\n";

  micro_data[1] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "2        var2  -5       DI_VERSION   2       2     INSERT    NORMAL   CF\n"
      "2        var2  -5       0           -1      -1     DELETE    NORMAL   CL\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -10       DI_VERSION  -1       10     INSERT    NORMAL   CF\n"
      "0        var0  -10       0           -1       1      DELETE    NORMAL   CL\n"
      "2        var2  -10       DI_VERSION   2       12     INSERT    NORMAL   CF\n"
      "2        var2  -10       0            2       2      DELETE    NORMAL   CL\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -10       MIN         -1       10     INSERT    INSERT_DELETE   SCF\n"
      "0        var0  -10       DI_VERSION  -1       10     INSERT    NORMAL   C\n"
      "0        var0  -10       0           -1       1      DELETE    NORMAL   C\n"
      "0        var0  -8        DI_VERSION  -1       1      INSERT    NORMAL   C\n"
      "0        var0  -8        0           -1      -1      DELETE    NORMAL   CL\n"
      "1        var1  -9        DI_VERSION   10     -1      INSERT    NORMAL   CF\n"
      "1        var1  -9        0           -1      -1      DELETE    NORMAL   CL\n"
      "2        var2  -10       MIN          2       12     INSERT    INSERT_DELETE   SCF\n"
      "2        var2  -10       DI_VERSION   2       12     INSERT    NORMAL   C\n"
      "2        var2  -10       0            2       2      DELETE    NORMAL   C\n"
      "2        var2  -5        DI_VERSION   2       2      INSERT    NORMAL   C\n"
      "2        var2  -5        0           -1      -1      DELETE    NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, uncommit_rowkey_in_one_macro_committed_following_last)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "0        var0  -8       DI_VERSION   -1       1      INSERT    NORMAL   CF  trans_id_0\n"
      "0        var0  -8       0            -1      -1      DELETE    NORMAL   CL  trans_id_0\n"
      "1        var1  MIN       -99         10       3      INSERT    NORMAL   UCF trans_id_1\n"
      "1        var1  MIN       -98         -1       3      DELETE    NORMAL   UC  trans_id_1\n";

  micro_data[1] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -5        DI_VERSION  -1       3      INSERT    NORMAL   C\n"
      "1        var1  -5        0           -1      -1      DELETE    NORMAL   CL\n"
      "2        var2  -5        DI_VERSION   2       2      INSERT    NORMAL   CF\n"
      "2        var2  -5        0           -1      -1      DELETE    NORMAL   CL\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -10       DI_VERSION  -1      10      INSERT    NORMAL   CF\n"
      "0        var0  -10       0           -1       1      DELETE    NORMAL   CL\n"
      "2        var2  -10       DI_VERSION   2      12      INSERT    NORMAL   CF\n"
      "2        var2  -10       0            2       2      DELETE    NORMAL   CL\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  for (int64_t i = 1; i <= 1; i++) {
    ObTxData *tx_data = new ObTxData();
    transaction::ObTransID tx_id = i;

    // fill in data
    tx_data->tx_id_ = tx_id;
    tx_data->commit_version_.convert_for_tx(i * 10);
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
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -10       MIN         -1      10      INSERT    INSERT_DELETE   SCF\n"
      "0        var0  -10       DI_VERSION  -1      10      INSERT    NORMAL   C\n"
      "0        var0  -10       0           -1       1      DELETE    NORMAL   C\n"
      "0        var0  -8       DI_VERSION   -1       1      INSERT    NORMAL   C\n"
      "0        var0  -8       0            -1      -1      DELETE    NORMAL   CL\n"
      "1        var1  -10       MIN          10      3      INSERT    INSERT_DELETE   SCF\n"
      "1        var1  -10       DI_VERSION   10      3      INSERT    NORMAL   C\n"
      "1        var1  -10       0           -1       3      DELETE    NORMAL   C\n"
      "1        var1  -5        DI_VERSION  -1       3      INSERT    NORMAL   C\n"
      "1        var1  -5        0           -1      -1      DELETE    NORMAL   CL\n"
      "2        var2  -10       MIN          2      12      INSERT    INSERT_DELETE   SCF\n"
      "2        var2  -10       DI_VERSION   2      12      INSERT    NORMAL   C\n"
      "2        var2  -10       0            2       2      DELETE    NORMAL   C\n"
      "2        var2  -5        DI_VERSION   2       2      INSERT    NORMAL   C\n"
      "2        var2  -5        0           -1      -1      DELETE    NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, uncommit_rowkey_in_one_macro_committed_following_shadow)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "0        var0  -8       DI_VERSION   -1        1      INSERT     NORMAL   CF   trans_id_0\n"
      "0        var0  -8       0            -1       -1      DELETE     NORMAL   CL   trans_id_0\n"
      "1        var1  MIN      -99           10       3      INSERT     NORMAL   UCF  trans_id_1\n"
      "1        var1  MIN      -98           3        3      DELETE     NORMAL   UC   trans_id_1\n";

  micro_data[1] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -8       MIN           3        3       INSERT    INSERT_DELETE   SC\n"
      "1        var1  -8       DI_VERSION    3        3       INSERT    NORMAL   C\n"
      "1        var1  -8       0            -1        3       DELETE    NORMAL   C\n"
      "1        var1  -5       DI_VERSION   -1        3       INSERT    NORMAL   C\n"
      "1        var1  -5       0            -1       -1       DELETE    NORMAL   CL\n"
      "2        var2  -5       DI_VERSION    2        2       INSERT    NORMAL   CF\n"
      "2        var2  -5       0            -1       -1       DELETE    NORMAL   CL\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -10       DI_VERSION   -1        10      INSERT    NORMAL   CF\n"
      "0        var0  -10       0            -1        1       DELETE    NORMAL   CL\n"
      "2        var2  -10       DI_VERSION    2        12      INSERT    NORMAL   CF\n"
      "2        var2  -10       0             2        2       DELETE    NORMAL   CL\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  for (int64_t i = 1; i <= 1; i++) {
    ObTxData *tx_data = new ObTxData();
    transaction::ObTransID tx_id = i;

    // fill in data
    tx_data->tx_id_ = tx_id;
    tx_data->commit_version_.convert_for_tx(i * 10);
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
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -10       MIN          -1        10      INSERT    INSERT_DELETE   SCF\n"
      "0        var0  -10       DI_VERSION   -1        10      INSERT    NORMAL   C\n"
      "0        var0  -10       0            -1        1       DELETE    NORMAL   C\n"
      "0        var0  -8        DI_VERSION   -1        1       INSERT    NORMAL   C\n"
      "0        var0  -8        0            -1       -1       DELETE    NORMAL   CL\n"
      "1        var1  -10      MIN           10       3       INSERT    INSERT_DELETE   SCF\n"
      "1        var1  -10      DI_VERSION    10       3       INSERT    NORMAL   C\n"
      "1        var1  -10      0             3        3       DELETE    NORMAL   C\n"
      "1        var1  -8       DI_VERSION    3        3       INSERT    NORMAL   C\n"
      "1        var1  -8       0            -1        3       DELETE    NORMAL   C\n"
      "1        var1  -5       DI_VERSION   -1        3       INSERT    NORMAL   C\n"
      "1        var1  -5       0            -1       -1       DELETE    NORMAL   CL\n"
      "2        var2  -10      MIN            2        12     INSERT    INSERT_DELETE   SCF\n"
      "2        var2  -10       DI_VERSION    2        12      INSERT    NORMAL   C\n"
      "2        var2  -10       0             2        2       DELETE    NORMAL   C\n"
      "2        var2  -5       DI_VERSION    2        2       INSERT    NORMAL   C\n"
      "2        var2  -5       0            -1       -1       DELETE    NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, rowkey_cross_three_macro_full_merge)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[4];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -8       DI_VERSION    2       -1       INSERT    NORMAL   CF\n"
      "0        var0  -8       0            -1       -1       DELETE    NORMAL   CL\n"
      "1        var1  -8       MIN           2        5       INSERT    INSERT_DELETE   SCF\n"
      "1        var1  -8       DI_VERSION    2        5       INSERT    NORMAL   C\n"
      "1        var1  -8       0             2        4       DELETE    NORMAL   C\n";

  micro_data[1] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -7       DI_VERSION    2        4       INSERT    NORMAL   C\n"
      "1        var1  -7       0             2        3       DELETE    NORMAL   C\n";

  micro_data[2] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -6       DI_VERSION    2        3       INSERT    NORMAL   C\n"
      "1        var1  -6       0             2       -1       DELETE    NORMAL   C\n";

  micro_data[3] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -5       DI_VERSION    2       -1       INSERT    NORMAL   C\n"
      "1        var1  -5       0            -1       -1       DELETE    NORMAL   CL\n"
      "2        var2  -5       DI_VERSION    2       -1       INSERT    NORMAL   CF\n"
      "2        var2  -5       0            -1       -1       DELETE    NORMAL   CL\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_one_macro(&micro_data[2], 1);
  prepare_one_macro(&micro_data[3], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -10       DI_VERSION    2       10       INSERT    NORMAL   CF\n"
      "0        var0  -10       0             2       -1       DELETE    NORMAL   CL\n"
      "2        var2  -10       DI_VERSION    2       12       INSERT    NORMAL   CF\n"
      "2        var2  -10       0             2       -1       DELETE    NORMAL   CL\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, true/*is_full_merge*/, trans_version_range, merge_context);
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -10      MIN            2       10       INSERT    INSERT_DELETE   SCF\n"
      "0        var0  -10       DI_VERSION    2       10       INSERT    NORMAL   C\n"
      "0        var0  -10       0             2       -1       DELETE    NORMAL   C\n"
      "0        var0  -8       DI_VERSION    2       -1       INSERT    NORMAL   C\n"
      "0        var0  -8       0            -1       -1       DELETE    NORMAL   CL\n"
      "1        var1  -8       MIN           2        5       INSERT    INSERT_DELETE   SCF\n"
      "1        var1  -8       DI_VERSION    2        5       INSERT    NORMAL   C\n"
      "1        var1  -8       0             2        4       DELETE    NORMAL   C\n"
      "1        var1  -7       DI_VERSION    2        4       INSERT    NORMAL   C\n"
      "1        var1  -7       0             2        3       DELETE    NORMAL   C\n"
      "1        var1  -6       DI_VERSION    2        3       INSERT    NORMAL   C\n"
      "1        var1  -6       0             2       -1       DELETE    NORMAL   C\n"
      "1        var1  -5       DI_VERSION    2       -1       INSERT    NORMAL   C\n"
      "1        var1  -5       0            -1       -1       DELETE    NORMAL   CL\n"
      "2        var2  -10      MIN            2        12      INSERT   INSERT_DELETE   SCF\n"
      "2        var2  -10       DI_VERSION    2       12       INSERT    NORMAL   C\n"
      "2        var2  -10       0             2       -1       DELETE    NORMAL   C\n"
      "2        var2  -5       DI_VERSION    2       -1       INSERT    NORMAL   C\n"
      "2        var2  -5       0            -1       -1       DELETE    NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, test_merge_with_multi_trans)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -9       DI_VERSION    7       12       INSERT    NORMAL   CF\n"
      "0        var0  -9       0            -1       -1       DELETE    NORMAL   CL\n";

  micro_data[1] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   MIN     -12           9        8       INSERT    NORMAL   UCF  trans_id_4\n"
      "1        var1   MIN     -11           7        6       DELETE    NORMAL   UC   trans_id_4\n"
      "1        var1   -22     MIN           7        6       INSERT    INSERT_DELETE   SC   trans_id_0\n"
      "1        var1   -22     DI_VERSION    7        6       INSERT    NORMAL   C    trans_id_0\n"
      "1        var1   -22     0            -1        9       DELETE    NORMAL   C    trans_id_0\n";

  micro_data[2] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   -4       DI_VERSION   -1        9       INSERT    NORMAL   C    trans_id_0\n"
      "1        var1   -4       0            -1       -1       DELETE    NORMAL   CL   trans_id_0\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1, INT64_MAX, true);
  prepare_one_macro(&micro_data[2], 1, INT64_MAX, true);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "0        var0   MIN      -19          9         12      INSERT    NORMAL   UCF   trans_id_1\n"
      "0        var0   MIN      -18          7         12      DELETE    NORMAL   UCL    trans_id_1\n"
      "1        var1   MIN      -17          8         8       INSERT    NORMAL   UCF   trans_id_4\n"
      "1        var1   MIN      -16          9         8       DELETE    NORMAL   UCL    trans_id_4\n"
      "2        var2   MIN      -15          12        7       INSERT    NORMAL   UCF   trans_id_1\n"
      "2        var2   MIN      -14          -1        7       DELETE    NORMAL   UC     trans_id_1\n"
      "2        var2   -4       MIN          -1        7       INSERT    INSERT_DELETE   SC     trans_id_0\n"
      "2        var2   -4       DI_VERSION   -1        7       INSERT    NORMAL   C     trans_id_0\n"
      "2        var2   -4       0            -1       -1       DELETE    NORMAL   CL    trans_id_0\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1, INT64_MAX, true);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "2        var2  MIN       -25          18        7       INSERT    NORMAL   UCF   trans_id_1\n"
      "2        var2  MIN       -24          12        7       DELETE    NORMAL   UCL   trans_id_1\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  prepare_data_end(handle3);
  merge_context.static_param_.tables_handle_.add_table(handle3);
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  for (int64_t i = 1; i <= 4; i++) {
    ObTxData *tx_data = new ObTxData();
    ASSERT_EQ(OB_SUCCESS, tx_data->init_tx_op());
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
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "0        var0  -11      MIN          9         12      INSERT    INSERT_DELETE   SCF  trans_id_0\n"
      "0        var0  -11      DI_VERSION   9         12      INSERT    NORMAL   C   trans_id_0\n"
      "0        var0  -11      0            7         12      DELETE    NORMAL   C    trans_id_0\n"
      "0        var0  -9       DI_VERSION    7       12       INSERT    NORMAL   C    trans_id_0\n"
      "0        var0  -9       0            -1       -1       DELETE    NORMAL   CL   trans_id_0\n"
      "1        var1   MIN     -17          8         8       INSERT    NORMAL  UCF  trans_id_4\n"
      "1        var1   MIN     -16           9        8       DELETE    NORMAL   UC   trans_id_4\n"
      "1        var1   MIN     -12          9         8       INSERT    NORMAL   UC  trans_id_4\n"
      "1        var1   MIN     -11           7        6       DELETE    NORMAL   UC   trans_id_4\n"
      "1        var1   -22      MIN          7        6       INSERT    INSERT_DELETE   SC   trans_id_0\n"
      "1        var1   -22     DI_VERSION    7        6       INSERT    NORMAL   C    trans_id_0\n"
      "1        var1   -22     0            -1        9       DELETE    NORMAL   C    trans_id_0\n"
      "1        var1   -4       DI_VERSION   -1        9       INSERT    NORMAL   C    trans_id_0\n"
      "1        var1   -4       0            -1       -1       DELETE    NORMAL   CL   trans_id_0\n"
      "2        var2  -11     MIN            18        7       INSERT    INSERT_DELETE   SCF   trans_id_0\n"
      "2        var2  -11       DI_VERSION   18        7       INSERT    NORMAL   C    trans_id_0\n"
      "2        var2  -11       0            -1        7       DELETE    NORMAL   C     trans_id_0\n"
      "2        var2   -4       DI_VERSION   -1        7       INSERT    NORMAL   C     trans_id_0\n"
      "2        var2   -4       0            -1       -1       DELETE    NORMAL   CL    trans_id_0\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  handle3.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, test_merge_with_multi_trans_can_compact)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[4];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0   -9       DI_VERSION    7        12      INSERT    NORMAL   CF\n"
      "0        var0   -9       0            -1       -1       DELETE    NORMAL   CL\n";

  micro_data[1] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   -4       DI_VERSION    1        1       INSERT    NORMAL   CF     trans_id_0\n"
      "1        var1   -4       0            -1       -1       DELETE    NORMAL   CL     trans_id_0\n";

  micro_data[2] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "2        var2   MIN      -13          70       99       INSERT    NORMAL   UCF    trans_id_4\n"
      "2        var2   MIN      -12          70       88       DELETE    NORMAL   UC     trans_id_4\n"
      "2        var2   -20      MIN          70       88       INSERT    INSERT_DELETE   SC     trans_id_0\n";

  micro_data[3] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "2        var2   -20      DI_VERSION   70       88       INSERT    NORMAL   C     trans_id_0\n"
      "2        var2   -20      0            70       -1       DELETE    NORMAL   C     trans_id_0\n"
      "2        var2   -10      DI_VERSION   70       -1       INSERT    NORMAL   C     trans_id_0\n"
      "2        var2   -10      0            -1       -1       DELETE    NORMAL   CL    trans_id_0\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_one_macro(&micro_data[2], 1, INT64_MAX, true);
  prepare_one_macro(&micro_data[3], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[5];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "0        var0   -9       DI_VERSION   9       12       INSERT    NORMAL   CF      trans_id_0\n"
      "0        var0   -9        0           7       12       DELETE    NORMAL   CL      trans_id_0\n"
      "1        var1   MIN      -89          7        9       INSERT    NORMAL   UCF     trans_id_3\n"
      "1        var1   MIN      -88          7        8       DELETE    NORMAL   UC      trans_id_3\n";

  micro_data2[1] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1  MIN    -80             7        8       INSERT    NORMAL   UC    trans_id_3\n"
      "1        var1  MIN    -79             7        5       DELETE    NORMAL   UC    trans_id_3\n";

  micro_data2[2] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1  MIN    -70             7        5       INSERT    NORMAL   UC    trans_id_3\n"
      "1        var1  MIN    -69             2        5       DELETE    NORMAL   UC    trans_id_3\n";

  micro_data2[3] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1  MIN    -30             2        5       INSERT    NORMAL   UC    trans_id_3\n"
      "1        var1  MIN    -29             2        3       DELETE    NORMAL   UC    trans_id_3\n";

  micro_data2[4] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1  MIN    -10             2        3       INSERT    NORMAL   UC    trans_id_3\n"
      "1        var1  MIN     -9             2        2       DELETE    NORMAL   UC    trans_id_3\n"
      "1        var1   -5      DI_VERSION    2        2       INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -5      0             1        1       DELETE    NORMAL   CL    trans_id_0\n";

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
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  for (int64_t i = 1; i <= 5; i++) {
    ObTxData *tx_data = new ObTxData();
    ASSERT_EQ(OB_SUCCESS, tx_data->init_tx_op());
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
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "0        var0   -9       DI_VERSION    9        12      INSERT    NORMAL   CF   trans_id_0\n"
      "0        var0   -9       0            -1       -1       DELETE    NORMAL   CL   trans_id_0\n"
      "1        var1  -33       MIN           7        9       INSERT    INSERT_DELETE   SCF   trans_id_3\n"
      "1        var1  -33       DI_VERSION    7        9       INSERT    NORMAL   C     trans_id_3\n"
      "1        var1  -33       0             2        2       DELETE    NORMAL   C     trans_id_3\n"
      "1        var1   -5       DI_VERSION    2        2       INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -5       0             1        1       DELETE    NORMAL   C     trans_id_0\n"
      "1        var1   -4       DI_VERSION    1        1       INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -4       0            -1       -1       DELETE    NORMAL   CL    trans_id_0\n"
      "2        var2   -44      MIN          70       99       INSERT    INSERT_DELETE   SCF   trans_id_4\n"
      "2        var2   -44      DI_VERSION   70       99       INSERT    NORMAL   C     trans_id_4\n"
      "2        var2   -44      0            70       88       DELETE    NORMAL   C     trans_id_4\n"
      "2        var2   -20      DI_VERSION   70       88       INSERT    NORMAL   C     trans_id_0\n"
      "2        var2   -20      0            70       -1       DELETE    NORMAL   C     trans_id_0\n"
      "2        var2   -10      DI_VERSION   70       -1       INSERT    NORMAL   C     trans_id_0\n"
      "2        var2   -10      0            -1       -1       DELETE    NORMAL   CL    trans_id_0\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, test_merge_with_multi_trans_can_not_compact)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[5];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0   -9       MIN           7        12      INSERT    INSERT_DELETE   SCF\n"
      "0        var0   -9       DI_VERSION    7        12      INSERT    NORMAL   C\n"
      "0        var0   -9       0             -1       -1      DELETE    NORMAL   CL\n";

  micro_data[1] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   -44      MIN           7        59      INSERT    INSERT_DELETE   SCF   trans_id_0\n"
      "1        var1   -44      DI_VERSION    7        59      INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -44      0             7        28      DELETE    NORMAL   C     trans_id_0\n";

  micro_data[2] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   -33      DI_VERSION    7        28      INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -33      0             7        71      DELETE    NORMAL   C     trans_id_0\n";

  micro_data[3] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   -22      DI_VERSION    7        71      INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -22      0             7        2       DELETE    NORMAL   C     trans_id_0\n";

  micro_data[4] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   -11      DI_VERSION    7        2       INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -11      0            -1        9       DELETE    NORMAL   C     trans_id_0\n"
      "1        var1   -4       DI_VERSION   -1        9       INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -4       0            -1       -1       DELETE    NORMAL   CL    trans_id_0\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1, INT64_MAX, true);
  prepare_one_macro(&micro_data[2], 1, INT64_MAX, true);
  prepare_one_macro(&micro_data[3], 1, INT64_MAX, true);
  prepare_one_macro(&micro_data[4], 1, INT64_MAX, true);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "0        var0  -9        MIN           9        12      INSERT    INSERT_DELETE   SCF   trans_id_0\n"
      "0        var0  -9        DI_VERSION    9        12      INSERT    NORMAL   C     trans_id_0\n"
      "0        var0  -9        0             7        12      DELETE    NORMAL   CL    trans_id_0\n"
      "1        var1  MIN       -89           7        9       INSERT    NORMAL   UCF   trans_id_5\n"
      "1        var1  MIN       -88           7        100     DELETE    NORMAL   UC    trans_id_5\n"
      "1        var1  -44       MIN           7        100     INSERT    INSERT_DELETE   SC    trans_id_0\n"
      "1        var1  -44       DI_VERSION    7        100     INSERT    NORMAL   C     trans_id_0\n"
      "1        var1  -44       0             7        59      DELETE    NORMAL   CL    trans_id_0\n"
      "2        var2  MIN       -15          12        7       INSERT    NORMAL   UCF   trans_id_1\n"
      "2        var2  MIN       -14          -1        7       DELETE    NORMAL   UC    trans_id_1\n"
      "2        var2   -4       MIN          -1        7       INSERT    INSERT_DELETE   SC    trans_id_0\n"
      "2        var2   -4       DI_VERSION   -1        7       INSERT    NORMAL   C     trans_id_0\n"
      "2        var2   -4       0            -1       -1       DELETE    NORMAL   CL    trans_id_0\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1, INT64_MAX, true);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "2        var2  MIN       -25          18        7       INSERT    NORMAL   UCF    trans_id_1\n"
      "2        var2  MIN       -24          12        7       DELETE    NORMAL   UCL    trans_id_1\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(100);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  prepare_data_end(handle3);
  merge_context.static_param_.tables_handle_.add_table(handle3);
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  for (int64_t i = 1; i <= 5; i++) {
    ObTxData *tx_data = new ObTxData();
    ASSERT_EQ(OB_SUCCESS, tx_data->init_tx_op());
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
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "0        var0  -9        MIN           9        12      INSERT    INSERT_DELETE   SCF   trans_id_0\n"
      "0        var0  -9        DI_VERSION    9        12      INSERT    NORMAL   C     trans_id_0\n"
      "0        var0   -9       0             -1       -1      DELETE    NORMAL   CL    trans_id_0\n"
      "1        var1  MIN       -89           7        9       INSERT    NORMAL   UCF   trans_id_5\n"
      "1        var1  MIN       -88           7        100     DELETE    NORMAL   UC    trans_id_5\n"
      "1        var1  -44       MIN           7        100     INSERT    INSERT_DELETE   SC    trans_id_0\n"
      "1        var1  -44       DI_VERSION    7        100     INSERT    NORMAL   C     trans_id_0\n"
      "1        var1  -44       0             7        28      DELETE    NORMAL   C     trans_id_0\n"
      "1        var1   -33      DI_VERSION    7        28      INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -33      0             7        71      DELETE    NORMAL   C     trans_id_0\n"
      "1        var1   -22      DI_VERSION    7        71      INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -22      0             7        2       DELETE    NORMAL   C     trans_id_0\n"
      "1        var1   -11      DI_VERSION    7        2       INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -11      0            -1        9       DELETE    NORMAL   C     trans_id_0\n"
      "1        var1   -4       DI_VERSION   -1        9       INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -4       0            -1       -1       DELETE    NORMAL   CL    trans_id_0\n"
      "2        var2  -11       MIN          18        7       INSERT    INSERT_DELETE   SCF   trans_id_0\n"
      "2        var2  -11       DI_VERSION   18        7       INSERT    NORMAL   C     trans_id_0\n"
      "2        var2  -11       0            -1        7       DELETE    NORMAL   C     trans_id_0\n"
      "2        var2   -4       DI_VERSION   -1        7       INSERT    NORMAL   C     trans_id_0\n"
      "2        var2   -4       0            -1       -1       DELETE    NORMAL   CL    trans_id_0\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  handle3.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, test_merge_with_macro_reused_with_shadow)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0   -9       MIN           7        12      INSERT    INSERT_DELETE   SCF\n"
      "0        var0   -9       DI_VERSION    7        12      INSERT    NORMAL   C\n"
      "0        var0   -9       0             -1       -1      DELETE    NORMAL   CL\n"
      "1        var1   -44      MIN           7        59      INSERT    INSERT_DELETE   SCF\n"
      "1        var1   -44      DI_VERSION    7        59      INSERT    NORMAL   C\n"
      "1        var1   -44      0             7        28      DELETE    NORMAL   C\n"
      "1        var1   -33      DI_VERSION    7        28      INSERT    NORMAL   C\n"
      "1        var1   -33      0             7        2       DELETE    NORMAL   C\n";

  micro_data[1] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1   -11      DI_VERSION    7        2       INSERT    NORMAL   C\n"
      "1        var1   -11      0            -1        9       DELETE    NORMAL   C\n"
      "1        var1   -4       DI_VERSION   -1        9       INSERT    NORMAL   C\n"
      "1        var1   -4       0            -1       -1       DELETE    NORMAL   CL\n"
      "2        var2   -4       MIN           7        7       INSERT    INSERT_DELETE   SCF\n"
      "2        var2   -4       DI_VERSION    7        7       INSERT    NORMAL   C\n"
      "2        var2   -4       0            -1       -1       DELETE    NORMAL   CL\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "2        var2   -30       MIN           7        20      INSERT    INSERT_DELETE   SCF\n"
      "2        var2   -30       DI_VERSION    7        20      INSERT    NORMAL   C\n"
      "2        var2   -30       0             7        10      DELETE    NORMAL   C\n"
      "2        var2   -20       DI_VERSION    7        10      INSERT    NORMAL   C\n"
      "2        var2   -20       0             7        7       DELETE    NORMAL   CL\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
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
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0   -9      MIN            7        12      INSERT    INSERT_DELETE   SCF\n"
      "0        var0   -9       DI_VERSION    7        12      INSERT    NORMAL   C\n"
      "0        var0   -9       0             -1       -1      DELETE    NORMAL   CL\n"
      "1        var1   -44      MIN           7        59      INSERT    INSERT_DELETE   SCF\n"
      "1        var1   -44      DI_VERSION    7        59      INSERT    NORMAL   C\n"
      "1        var1   -44      0             7        28      DELETE    NORMAL   C\n"
      "1        var1   -33      DI_VERSION    7        28      INSERT    NORMAL   C\n"
      "1        var1   -33      0             7        2       DELETE    NORMAL   C\n"
      "1        var1   -11      DI_VERSION    7        2       INSERT    NORMAL   C\n"
      "1        var1   -11      0            -1        9       DELETE    NORMAL   C\n"
      "1        var1   -4       DI_VERSION   -1        9       INSERT    NORMAL   C\n"
      "1        var1   -4       0            -1       -1       DELETE    NORMAL   CL\n"
      "2        var2   -30       MIN           7        20      INSERT    INSERT_DELETE   SCF\n"
      "2        var2   -30       DI_VERSION    7        20      INSERT    NORMAL   C\n"
      "2        var2   -30       0             7        10      DELETE    NORMAL   C\n"
      "2        var2   -20       DI_VERSION    7        10      INSERT    NORMAL   C\n"
      "2        var2   -20       0             7        7       DELETE    NORMAL   C\n"
      "2        var2   -4       DI_VERSION    7        7       INSERT    NORMAL   C\n"
      "2        var2   -4       0            -1       -1       DELETE    NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, test_merge_with_macro_reused_without_shadow)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0   -9       MIN           7       12       INSERT    INSERT_DELETE   SCF\n"
      "0        var0   -9       DI_VERSION    7       12       INSERT    NORMAL   C\n"
      "0        var0   -9       0            -1       -1       DELETE    NORMAL   CL\n"
      "1        var1   -44      MIN           7       59       INSERT    INSERT_DELETE   SCF\n"
      "1        var1   -44      DI_VERSION    7       59       INSERT    NORMAL   C\n"
      "1        var1   -44      0             7       28       DELETE    NORMAL   C\n"
      "1        var1   -33      DI_VERSION    7       28       INSERT    NORMAL   C\n"
      "1        var1   -33      0            -1       -1       DELETE    NORMAL   CL\n";

  micro_data[1] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "2        var2   MIN      -10           20       9       INSERT    NORMAL   UCF trans_id_2\n"
      "2        var2   MIN       -9           9        9       DELETE    NORMAL   UC  trans_id_2\n"
      "2        var2   -20      MIN           9        9       INSERT    INSERT_DELETE   SC  trans_id_0\n"
      "2        var2   -20      DI_VERSION    9        9       INSERT    NORMAL   C   trans_id_0\n"
      "2        var2   -20      0             2        9       DELETE    NORMAL   C   trans_id_0\n"
      "2        var2   -10      DI_VERSION    2        9       INSERT    NORMAL   C   trans_id_0\n"
      "2        var2   -10      0            -1       -1       DELETE    NORMAL   CL  trans_id_0\n"
      "3        var3   -10      MIN           3        3       INSERT    INSERT_DELETE   SCF trans_id_0\n"
      "3        var3   -10      DI_VERSION    3        3       INSERT    NORMAL   C   trans_id_0\n"
      "3        var3   -10      0            -1       -1       DELETE    NORMAL   CL  trans_id_0\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1, INT64_MAX, true);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "3        var3   -30      MIN           30       3       INSERT    INSERT_DELETE   SCF\n"
      "3        var3   -30      DI_VERSION    30       3       INSERT    NORMAL   C\n"
      "3        var3   -30      0             3        3       DELETE    NORMAL   CL\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  get_tx_table_guard(tx_table_guard);
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
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0   -9       MIN           7       12       INSERT    INSERT_DELETE   SCF\n"
      "0        var0   -9       DI_VERSION    7       12       INSERT    NORMAL   C\n"
      "0        var0   -9       0            -1       -1       DELETE    NORMAL   CL\n"
      "1        var1   -44      MIN           7       59       INSERT    INSERT_DELETE   SCF\n"
      "1        var1   -44      DI_VERSION    7       59       INSERT    NORMAL   C\n"
      "1        var1   -44      0             7       28       DELETE    NORMAL   C\n"
      "1        var1   -33      DI_VERSION    7       28       INSERT    NORMAL   C\n"
      "1        var1   -33      0            -1       -1       DELETE    NORMAL   CL\n"
      "2        var2   -22      MIN           20       9       INSERT    INSERT_DELETE   SCF\n"
      "2        var2   -22      DI_VERSION    20       9       INSERT    NORMAL   C\n"
      "2        var2   -22      0             9        9       DELETE    NORMAL   C\n"
      "2        var2   -20      DI_VERSION    9        9       INSERT    NORMAL   C\n"
      "2        var2   -20      0             2        9       DELETE    NORMAL   C\n"
      "2        var2   -10      DI_VERSION    2        9       INSERT    NORMAL   C\n"
      "2        var2   -10      0            -1       -1       DELETE    NORMAL   CL\n"
      "3        var3   -30      MIN           30       3       INSERT    INSERT_DELETE   SCF\n"
      "3        var3   -30      DI_VERSION    30       3       INSERT    NORMAL   C\n"
      "3        var3   -30      0             3        3       DELETE    NORMAL   C\n"
      "3        var3   -10      DI_VERSION    3        3       INSERT    NORMAL   C\n"
      "3        var3   -10      0            -1       -1       DELETE    NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, test_merge_with_greater_multi_version)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0   -9       MIN           7       12       INSERT    INSERT_DELETE   SCF\n"
      "0        var0   -9       DI_VERSION    7       12       INSERT    NORMAL   C\n"
      "0        var0   -9       0             7        7       DELETE    NORMAL   C\n"
      "0        var0   -8       DI_VERSION    7        7       INSERT    NORMAL   C\n"
      "0        var0   -8       0            -1       -1       DELETE    NORMAL   CL\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0   -20      MIN           15      15       INSERT    INSERT_DELETE   SCF\n"
      "0        var0   -20      DI_VERSION    15      15       INSERT    NORMAL   C\n"
      "0        var0   -20      0             15      12       DELETE    NORMAL   C\n"
      "0        var0   -17      DI_VERSION    15      12       INSERT    NORMAL   C\n"
      "0        var0   -17      0             7       12       DELETE    NORMAL   CL\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0   -20      MIN           15      15       INSERT    INSERT_DELETE   SCF\n"
      "0        var0   -20      DI_VERSION    15      15       INSERT    NORMAL   C\n"
      "0        var0   -17      0              7      12       DELETE    NORMAL   C\n"
      "0        var0    -9      DI_VERSION     7      12       INSERT    NORMAL   C\n"
      "0        var0    -8      0             -1      -1       INSERT    NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, test_merge_with_greater_multi_version_and_uncommit)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "0        var0   MIN      -20           7       30       INSERT    NORMAL   UCF  trans_id_1\n"
      "0        var0   MIN      -19           7       12       DELETE    NORMAL   UC   trans_id_1\n"
      "0        var0   -9       MIN           7       12       INSERT    INSERT_DELETE   SC   trans_id_0\n"
      "0        var0   -9       DI_VERSION    7       12       INSERT    NORMAL   C    trans_id_0\n"
      "0        var0   -9       0             7        7       DELETE    NORMAL   C    trans_id_0\n"
      "0        var0   -8       DI_VERSION    7        7       INSERT    NORMAL   C    trans_id_0\n"
      "0        var0   -8       0            -1       -1       DELETE    NORMAL   CL   trans_id_0\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1, INT64_MAX, true);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0   -20      MIN           7       15       INSERT    INSERT_DELETE   SCF\n"
      "0        var0   -20      DI_VERSION    7       15       INSERT    NORMAL   C\n"
      "0        var0   -20      0             7       13       DELETE    NORMAL   C\n"
      "0        var0   -17      DI_VERSION    7       13       INSERT    NORMAL   C\n"
      "0        var0   -17      0             7       30       DELETE    NORMAL   CL\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 50;
  trans_version_range.base_version_ = 1;

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  get_tx_table_guard(tx_table_guard);
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
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0   -20      MIN             7       15       INSERT    INSERT_DELETE   SCF\n"
      "0        var0   -20      DI_VERSION      7       15       INSERT    NORMAL   C\n"
      "0        var0   -17      0               7       30       DELETE    NORMAL   C\n"
      "0        var0   -11      DI_VERSION      7       30       INSERT    NORMAL   C\n"
      "0        var0   -8       0              -1       -1       DELETE    NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, test_merge_with_ghost_row)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   MIN      -9      18       2        INSERT    NORMAL   UCF   trans_id_1\n"
      "1        var1   MIN      -8      -1       19       DELETE    NORMAL   UC    trans_id_1\n"
      "1        var1   MIN      -7      -1       19       INSERT    NORMAL   UC    trans_id_1\n"
      "1        var1   MIN      -6      -1       -1       DELETE    NORMAL   UCL   trans_id_1\n"
      "2        var2   MIN      -11     18       -1       INSERT    NORMAL   UCF   trans_id_2\n"
      "2        var2   MIN      -10     -1       -1       DELETE    NORMAL   UCL   trans_id_2\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1, INT64_MAX, true);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   MIN      -13      18       59       INSERT    NORMAL   UCF   trans_id_1\n"
      "1        var1   MIN      -12      18        2       DELETE    NORMAL   UCL   trans_id_1\n"
      "2        var2   MIN      -15      18       59       INSERT    NORMAL   UCF   trans_id_2\n"
      "2        var2   MIN      -14      18       -1       DELETE    NORMAL   UCL   trans_id_2\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1, INT64_MAX, true);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   MIN      -17      -1       59       INSERT    NORMAL   UCF   trans_id_3\n"
      "1        var1   MIN      -16      -1       -1       DELETE    NORMAL   UCL   trans_id_3\n"
      "2        var2   MIN      -19      18       1        INSERT    NORMAL   UCF   trans_id_4\n"
      "2        var2   MIN      -18      18       59       DELETE    NORMAL   UC    trans_id_4\n"
      "2        var2   MAGIC    MAGIC    NOP      NOP      EXIST     NORMAL   LG    trans_id_0\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1, INT64_MAX, true);
  prepare_data_end(handle3);
  merge_context.static_param_.tables_handle_.add_table(handle3);
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  get_tx_table_guard(tx_table_guard);
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
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "2        var2  -44       MIN             18       1        INSERT    INSERT_DELETE   SCF\n"
      "2        var2  -44       DI_VERSION      18       1        INSERT    NORMAL   C\n"
      "2        var2  -44       0               18       59       DELETE    NORMAL   C\n"
      "2        var2  -22       DI_VERSION      18       59       INSERT    NORMAL   C\n"
      "2        var2  -22       0               -1       -1       DELETE    NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, compare_dml_flag)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -8       0             1        1       INSERT    NORMAL   CLF\n"
      "2        var2  -6       0             2        2       INSERT    NORMAL   CLF\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -10       MIN           1        10      INSERT    INSERT_DELETE   SCF\n"
      "0        var0  -10       DI_VERSION    1        10      INSERT    NORMAL   C\n"
      "0        var0  -10       0             1        1       DELETE    NORMAL   CL\n"
      "2        var2  -12       0             2        2       DELETE    NORMAL   CLF\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -10      MIN              1        10      INSERT    INSERT_DELETE   SCF\n"
      "0        var0  -10      DI_VERSION       1        10      INSERT    NORMAL   C\n"
      "0        var0  -10       0               1        1       DELETE    NORMAL   C\n"
      "0        var0  -8       DI_VERSION       1        1       INSERT    NORMAL   CL\n"
      "2        var2  -12       MIN             2        2       DELETE    INSERT_DELETE   SCF\n"
      "2        var2  -12       0               2        2       DELETE    NORMAL   C\n"
      "2        var2  -6       DI_VERSION       2        2       INSERT    NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, get_last_after_reuse)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -15       MIN               -1       1      INSERT    INSERT_DELETE   SCF\n"
      "0        var0  -15       DI_VERSION        -1       1      INSERT    NORMAL   C\n"
      "0        var0  -15       0                 -1      -1      DELETE    NORMAL   CL\n"
      "1        var1  -15       MIN               12       2      INSERT    INSERT_DELETE   SCF\n"
      "1        var1  -15       DI_VERSION        12       2      INSERT    NORMAL   C\n"
      "1        var1  -15       0                 2        2      DELETE    NORMAL   C\n"
      "1        var1  -10       0                 2        2      INSERT    NORMAL   CL\n"
      "3        var3  -8        MIN               12      12      INSERT    INSERT_DELETE   SCF\n"
      "3        var3  -8        DI_VERSION        12      12       INSERT    NORMAL   C\n"
      "3        var3  -8        0                 2       12       DELETE    NORMAL   C\n";

  micro_data[1] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "3        var3  -6        DI_VERSION        2       12       INSERT    NORMAL   CL\n"
      "4        var4  -15       0                 2        2       INSERT    NORMAL   CLF\n"
      "5        var5  -15       DI_VERSION        -1      12       INSERT    NORMAL   CF\n"
      "5        var5  -15       0                 -1      -1       DELETE    NORMAL   CL\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "5        var5  -20       DI_VERSION       -1      13       INSERT    NORMAL   CF\n"
      "5        var5  -20       0                -1      12       DELETE    NORMAL   CL\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -15       MIN             -1       1      INSERT    INSERT_DELETE   SCF\n"
      "0        var0  -15       DI_VERSION      -1       1      INSERT    NORMAL   C\n"
      "0        var0  -15       0               -1      -1      DELETE    NORMAL   CL\n"
      "1        var1  -15       MIN             12       2      INSERT    INSERT_DELETE   SCF\n"
      "1        var1  -15       DI_VERSION      12       2      INSERT    NORMAL   C\n"
      "1        var1  -15       0               2        2      DELETE    NORMAL   C\n"
      "1        var1  -10       DI_VERSION      2        2      INSERT    NORMAL   CL\n"
      "3        var3  -8        MIN             12      12      INSERT    INSERT_DELETE   SCF\n"
      "3        var3  -8        DI_VERSION      12      12       INSERT    NORMAL   C\n"
      "3        var3  -8        0               2       12       DELETE    NORMAL   C\n"
      "3        var3  -6        DI_VERSION      2       12       INSERT    NORMAL   CL\n"
      "4        var4  -15       DI_VERSION      2        2       INSERT    NORMAL   CLF\n"
      "5        var5  -20       MIN             -1      13       INSERT    INSERT_DELETE   SCF\n"
      "5        var5  -20       DI_VERSION      -1      13       INSERT    NORMAL   C\n"
      "5        var5  -20       0               -1      12       DELETE    NORMAL   C\n"
      "5        var5  -15       DI_VERSION      -1      12       INSERT    NORMAL   C\n"
      "5        var5  -15       0               -1      -1       DELETE    NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}
TEST_F(TestMultiVersionDIMerge, rowkey_cross_two_macro_with_commit_scn_less_multi_version_start)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -8        0             2        2      INSERT    NORMAL   CLF\n";

  micro_data[1] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "2        var2  -8        MIN           3       2       INSERT    INSERT_DELETE   SCF\n"
      "2        var2  -8        DI_VERSION    3       2       INSERT    NORMAL   C\n"
      "2        var2  -8        0             2       2       DELETE    NORMAL   C\n";

  micro_data[2] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "2        var2  -6        0             2       2       INSERT    NORMAL   CL\n";
  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_one_macro(&micro_data[2], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -10       MIN           -1      10       INSERT    INSERT_DELETE   SCF\n"
      "0        var0  -10       DI_VERSION    -1      10       INSERT    NORMAL   C\n"
      "0        var0  -10       0             -1      -1       DELETE    NORMAL   CL\n"
      "3        var3  -10       MIN           -1      13       INSERT    INSERT_DELETE   SCF\n"
      "3        var3  -10       DI_VERSION    -1      13       INSERT    NORMAL   C\n"
      "3        var3  -10       0             -1      -1       DELETE    NORMAL   CL\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 10;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -10       DI_VERSION    -1      10       INSERT    NORMAL   CF\n"
      "0        var0  -10       0             -1      -1       DELETE    NORMAL   CL\n"
      "1        var1  -8        DI_VERSION     2       2       INSERT    NORMAL   CLF\n"
      "2        var2  -8        DI_VERSION     3       2       INSERT    NORMAL   CLF\n"
      "3        var3  -10       DI_VERSION    -1      13       INSERT    NORMAL   CF\n"
      "3        var3  -10       0             -1      -1       DELETE    NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, rowkey_cross_macro_with_last_shadow_version_less_than_multi_version)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1  -8       0               2       2       INSERT    NORMAL   CLF    trans_id_0\n";

  micro_data[1] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "2        var2  MIN       -11            3       9       INSERT    NORMAL   UCF    trans_id_2\n"
      "2        var2  MIN       -10            3       2       DELETE    NORMAL   UC     trans_id_2\n"
      "2        var2  -8        MIN            3       2       INSERT    INSERT_DELETE   SC     trans_id_0\n";

  micro_data[2] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "2        var2  -8        DI_VERSION     3       2       INSERT    NORMAL   C     trans_id_0\n"
      "2        var2  -8        0              2       2       DELETE    NORMAL   C     trans_id_0\n"
      "2        var2  -6        DI_VERSION     2       2       INSERT    NORMAL   C     trans_id_0\n"
      "2        var2  -6        0             -1      -1       DELETE    NORMAL   CL    trans_id_0\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_one_macro(&micro_data[2], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -32       MIN            2      10       INSERT    INSERT_DELETE   SCF\n"
      "1        var1  -32       DI_VERSION     2      10       INSERT    NORMAL   C\n"
      "1        var1  -32       0              2      2        DELETE    NORMAL   CL\n"
      "3        var3  -40       MIN           -1      13       INSERT    INSERT_DELETE   SCF\n"
      "3        var3  -40       DI_VERSION    -1      13       INSERT    NORMAL   C\n"
      "3        var3  -40       0             -1      -1       DELETE    NORMAL   CL\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  get_tx_table_guard(tx_table_guard);
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
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -32       MIN          2        10       INSERT    INSERT_DELETE   SCF\n"
      "1        var1  -32       DI_VERSION     2      10       INSERT    NORMAL   C\n"
      "1        var1  -32       0              2       2       DELETE    NORMAL   C\n"
      "1        var1  -8       DI_VERSION      2       2       INSERT    NORMAL   CL\n"
      "2        var2  -22      MIN            3       9        INSERT    INSERT_DELETE   SCF\n"
      "2        var2  -22      DI_VERSION     3       9        INSERT    NORMAL   C\n"
      "2        var2  -22      0              3       2        DELETE    NORMAL   C\n"
      "2        var2  -8        DI_VERSION     3       2       INSERT    NORMAL   C\n"
      "2        var2  -6        0             -1      -1       DELETE    NORMAL   CL\n"
      "3        var3  -40       MIN           -1      13       INSERT    INSERT_DELETE   SCF\n"
      "3        var3  -40       DI_VERSION    -1      13       INSERT    NORMAL   C\n"
      "3        var3  -40       0             -1      -1       DELETE    NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, shadow_row_is_last_in_macro)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -8       0             2        2       INSERT    NORMAL   CLF\n";

  micro_data[1] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "2        var2  -10       MIN           3       3       INSERT    INSERT_DELETE   SCF\n";

  micro_data[2] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "2        var2  -10       DI_VERSION    3       3       INSERT    NORMAL   C\n"
      "2        var2  -10       0             2       3       DELETE    NORMAL   C\n"
      "2        var2  -8        DI_VERSION    2       3       INSERT    NORMAL   C\n"
      "2        var2  -8        0             2       2       DELETE    NORMAL   C\n"
      "2        var2  -6        0             2       2       INSERT    NORMAL   CL\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_one_macro(&micro_data[2], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -32       MIN            2        10      INSERT    INSERT_DELETE   SCF\n"
      "1        var1  -32       DI_VERSION     2        10      INSERT    NORMAL   C\n"
      "1        var1  -32       0              2        2       DELETE    NORMAL   CL\n"
      "2        var2  -10       MIN            3        3       INSERT    INSERT_DELETE   SCF\n"
      "2        var2  -10       DI_VERSION     3        3       INSERT    NORMAL   C\n"
      "2        var2  -10       0              2        3       DELETE    NORMAL   C\n"
      "2        var2  -8        DI_VERSION     2        3       INSERT    NORMAL   C\n"
      "2        var2  -8        0              2        2       DELETE    NORMAL   C\n"
      "2        var2  -6        0              2        2       INSERT    NORMAL   CL\n"
      "3        var3  -40       MIN           -1       13       INSERT    INSERT_DELETE   SCF\n"
      "3        var3  -40       DI_VERSION    -1       13       INSERT    NORMAL   C\n"
      "3        var3  -40       0             -1       -1       DELETE    NORMAL   CL\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -32       MIN            2        10      INSERT    INSERT_DELETE   SCF\n"
      "1        var1  -32       DI_VERSION     2        10      INSERT    NORMAL   C\n"
      "1        var1  -32       0              2        2       DELETE    NORMAL   C\n"
      "1        var1  -8        DI_VERSION     2        2       INSERT    NORMAL   CL\n"
      "2        var2  -10       MIN            3        3       INSERT    INSERT_DELETE   SCF\n"
      "2        var2  -10       DI_VERSION     3        3       INSERT    NORMAL   C\n"
      "2        var2  -10       0              2        3       DELETE    NORMAL   C\n"
      "2        var2  -8        DI_VERSION     2        3       INSERT    NORMAL   C\n"
      "2        var2  -8        0              2        2       DELETE    NORMAL   C\n"
      "2        var2  -6        DI_VERSION     2        2       INSERT    NORMAL   CL\n"
      "3        var3  -40       MIN           -1       13       INSERT    INSERT_DELETE   SCF\n"
      "3        var3  -40       DI_VERSION    -1       13       INSERT    NORMAL   C\n"
      "3        var3  -40       0             -1       -1       DELETE    NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, rowkey_cross_macro_without_open_next_macro)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "0        var0  -10       DI_VERSION     3        2       INSERT    NORMAL   CLF    trans_id_0\n"
      "2        var2  MIN       -11            3        9       INSERT    NORMAL   UCF    trans_id_2\n"
      "2        var2  MIN       -10            3        2       DELETE    NORMAL   UC     trans_id_2\n";

  micro_data[1] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "2        var2  -8       MIN             3        2       INSERT    INSERT_DELETE   SC\n"
      "2        var2  -8        DI_VERSION     3        2       INSERT    NORMAL   C\n"
      "2        var2  -8        0              2        2       DELETE    NORMAL   C\n"
      "2        var2  -6        DI_VERSION     2        2       INSERT    NORMAL   CL\n";

  micro_data[2] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "3        var3  -8        DI_VERSION     3        2       INSERT    NORMAL   CF\n"
      "3        var3  -8        0             -1       -1       DELETE    NORMAL   CL\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_one_macro(&micro_data[2], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -32       DI_VERSION    -1        10      INSERT    NORMAL   CF\n"
      "1        var1  -32       0             -1        -1      DELETE    NORMAL   CL\n"
      "2        var2  -40       DI_VERSION     3        3       INSERT    NORMAL   CF\n"
      "2        var2  -40       0              3        2       DELETE    NORMAL   CL\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  get_tx_table_guard(tx_table_guard);
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
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
    "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
    "0        var0  -10       DI_VERSION     3        2       INSERT    NORMAL   CLF\n"
    "1        var1  -32       DI_VERSION     -1       10      INSERT    NORMAL   CF\n"
    "1        var1  -32       0              -1       -1      DELETE    NORMAL   CL\n"
    "2        var2  -40       MIN            3        3       INSERT    INSERT_DELETE   SCF\n"
    "2        var2  -40       DI_VERSION     3        3       INSERT    NORMAL   C\n"
    "2        var2  -40       0              3        2       DELETE    NORMAL   C\n"
    "2        var2  -8        DI_VERSION     3        2       INSERT    NORMAL   C\n"
    "2        var2  -8        0              2        2       DELETE    NORMAL   C\n"
    "2        var2  -6        DI_VERSION     2        2       INSERT    NORMAL   CL\n"
    "3        var3  -8        DI_VERSION     3        2       INSERT    NORMAL   CF\n"
    "3        var3  -8        0             -1       -1       DELETE    NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, range_cross_macro)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
    "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -10       DI_VERSION     2         2      INSERT    NORMAL   CLF\n"
      "2        var2  -20       MIN            15        15     INSERT    INSERT_DELETE   SCF\n"
      "2        var2  -20       DI_VERSION     15        15     INSERT    NORMAL   C\n"
      "2        var2  -20       0              15        2      DELETE    NORMAL   C\n";

  micro_data[1] =
    "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "2        var2  -10       DI_VERSION     15        2      INSERT    NORMAL   C\n"
      "2        var2  -10       0              2         2      DELETE    NORMAL   C\n"
      "2        var2  -8        DI_VERSION     2         2      INSERT    NORMAL   CL\n"
      "3        var3  -20       MIN            25       25      INSERT    INSERT_DELETE   SCF\n"
      "3        var3  -20       DI_VERSION     25       25      INSERT    NORMAL   C\n"
      "3        var3  -20       0              7        25      DELETE    NORMAL   C\n";

  micro_data[2] =
    "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "3        var3  -10       DI_VERSION     7        25      INSERT    NORMAL   C\n"
      "3        var3  -10       0              5        5       DELETE    NORMAL   C\n"
      "3        var3  -5        DI_VERSION     5        5       INSERT    NORMAL   CL\n"
      "5        var5  -5        DI_VERSION     7        7       INSERT    NORMAL   CF\n"
      "5        var5  -5        0             -1       -1       DELETE    NORMAL   CL\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 2);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
    "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -32       DI_VERSION    -1        10      INSERT    NORMAL   CF\n"
      "1        var1  -32       0             -1       -1       DELETE    NORMAL   CL\n"
      "4        var4  -40       DI_VERSION    -1        3       INSERT    NORMAL   CF\n"
      "4        var4  -40       0             -1       -1       DELETE    NORMAL   CL\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
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
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
    "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "3        var3  -20       MIN            25       25      INSERT    INSERT_DELETE   SCF\n"
      "3        var3  -20       DI_VERSION     25       25      INSERT    NORMAL   C\n"
      "3        var3  -20       0              7        25      DELETE    NORMAL   C\n"
      "3        var3  -10       DI_VERSION     7        25      INSERT    NORMAL   C\n"
      "3        var3  -10       0              5        5       DELETE    NORMAL   C\n"
      "3        var3  -5        DI_VERSION     5        5       INSERT    NORMAL   CL\n"
      "4        var4  -40       DI_VERSION    -1        3       INSERT    NORMAL   CF\n"
      "4        var4  -40       0             -1       -1       DELETE    NORMAL   CL\n"
      "5        var5  -5        DI_VERSION     7        7       INSERT    NORMAL   CF\n"
      "5        var5  -5        0             -1       -1       DELETE    NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, test_merge_base_iter_have_ghost_row)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
    "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   MIN      -8             18        2       INSERT    NORMAL   UCF     trans_id_1\n"
      "1        var1   MIN      -7             -1       19       DELETE    NORMAL   UC     trans_id_1\n"
      "1        var1   MIN      -6             -1       19       INSERT    NORMAL   UC     trans_id_1\n"
      "1        var1   MIN      -5             -1       -1       DELETE    NORMAL   UCL    trans_id_1\n"
      "2        var2   MIN      -9             18        0       INSERT    NORMAL   UCF    trans_id_2\n"
      "2        var2   MIN      -8             -1       -1       DELETE    NORMAL   UCL    trans_id_2\n";
  micro_data[1] =
    "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "3        var3   MIN      -71            18        1       INSERT    NORMAL   UCF    trans_id_1\n"
      "3        var3   MIN      -70            -1       -1       DELETE    NORMAL   UCL    trans_id_1\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1, INT64_MAX, true);
  prepare_one_macro(&micro_data[1], 1, INT64_MAX, true);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
    "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   MIN      -40            -1       59       INSERT    NORMAL   UCF    trans_id_3\n"
      "1        var1   MIN      -39            -1       -1       DELETE    NORMAL   UCL    trans_id_3\n"
      "2        var2   MIN      -38            18       59       INSERT    NORMAL   UCF    trans_id_2\n"
      "2        var2   MIN      -37            18        0       DELETE    NORMAL   UCL    trans_id_2\n"
      "3        var3   -15      DI_VERSION     18        1       INSERT    NORMAL   CF     trans_id_2\n"
      "3        var3   -15      0              -1       -1       DELETE    NORMAL   CL     trans_id_2\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1, INT64_MAX, true);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
    "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   MIN      -48            -1       59       INSERT    NORMAL   UCF   trans_id_3\n"
      "1        var1   MIN      -47            -1       -1       DELETE    NORMAL   UCL   trans_id_3\n"
      "2        var2   MIN      -71            18        1       INSERT    NORMAL   UCF   trans_id_4\n"
      "2        var2   MIN      -70            18       59       DELETE    NORMAL   UC    trans_id_4\n"
      "2        var2   MAGIC    MAGIC         NOP       NOP      EXIST     NORMAL   LG    trans_id_0\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1, INT64_MAX, true);
  prepare_data_end(handle3);
  merge_context.static_param_.tables_handle_.add_table(handle3);
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  get_tx_table_guard(tx_table_guard);
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
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
    "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "2        var2   -44      MIN            18       1        INSERT    INSERT_DELETE   SCF\n"
      "2        var2   -44      DI_VERSION     18       1        INSERT    NORMAL   C\n"
      "2        var2   -44      0              18       59       DELETE    NORMAL   C\n"
      "2        var2   -22      DI_VERSION     18       59       INSERT    NORMAL   C\n"
      "2        var2   -22      0              -1       -1       DELETE    NORMAL   CL\n"
      "3        var3   -15      DI_VERSION     18        1       INSERT    NORMAL   CF\n"
      "3        var3   -15      0              -1       -1       DELETE    NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, test_trans_cross_macro_with_ghost_row)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
    "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   MIN     -9            -1        6       INSERT    NORMAL   UCF       trans_id_1\n"
      "1        var1   MIN     -8            -1        7       DELETE    NORMAL   UC        trans_id_1\n";

  micro_data[1] =
    "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1   MAGIC   MAGIC         NOP       NOP     EXIST     NORMAL   LG\n"
      "2        var2   -26     DI_VERSION    -1        7       INSERT    NORMAL   CF\n"
      "2        var2   -26     0             -1       -1       DELETE    NORMAL   CL\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[2];
  micro_data2[0] =
    "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   MIN     -7             8        6       INSERT    NORMAL   UCF     trans_id_2\n"
      "1        var1   MIN     -6            -1        6       DELETE    NORMAL   UCL     trans_id_2\n";

  micro_data2[1] =
    "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "3        var3   -46     DI_VERSION    -1       18       INSERT    NORMAL   CF\n"
      "3        var3   -46     0             -1       -1       DELETE    NORMAL   CL\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_one_macro(&micro_data2[1], 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  for (int64_t i = 1; i < 3; i++) {
    ObTxData *tx_data = new ObTxData();
    transaction::ObTransID tx_id = i;

    // fill in data
    tx_data->tx_id_ = tx_id;
    tx_data->commit_version_.convert_for_tx(i * 20 + 9);
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
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
    "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   -49     MIN            8        6       INSERT    INSERT_DELETE   SCF\n"
      "1        var1   -49     DI_VERSION     8        6       INSERT    NORMAL   C\n"
      "1        var1   -49     0             -1        6       DELETE    NORMAL   C\n"
      "1        var1   -29     DI_VERSION    -1        6       INSERT    NORMAL   C\n"
      "1        var1   -29     0             -1        7       DELETE    NORMAL   CL\n"
      "2        var2   -26     DI_VERSION    -1        7       INSERT    NORMAL   CF\n"
      "2        var2   -26     0             -1       -1       DELETE    NORMAL   CL\n"
      "3        var3   -46     DI_VERSION    -1       18       INSERT    NORMAL   CF\n"
      "3        var3   -46     0             -1       -1       DELETE    NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, test_trans_cross_macro_with_ghost_row2)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
    "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   MIN     -9             8        5       INSERT    NORMAL   UCF    trans_id_1\n"
      "1        var1   MIN     -8             9        5       DELETE    NORMAL   UC     trans_id_1\n"
      "1        var1   -17     MIN            9        5       INSERT    INSERT_DELETE   SC     trans_id_0\n"
      "1        var1   -17     DI_VERSION     9        5       INSERT    NORMAL   C      trans_id_0\n"
      "1        var1   -17     0              9       -1       DELETE    NORMAL   C      trans_id_0\n"
      "1        var1   -12     DI_VERSION     9       -1       INSERT    NORMAL   C      trans_id_0\n"
      "1        var1   -12     0             -1       -1       DELETE    NORMAL   CL     trans_id_0\n";

  micro_data[1] =
    "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "3        var3   -16     MIN           18       -1       INSERT    INSERT_DELETE   SCF\n"
      "3        var3   -16     DI_VERSION    18       -1       INSERT    NORMAL   C\n"
      "3        var3   -16     0             -1       -1       DELETE    NORMAL   CL\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[2];
  micro_data2[0] =
    "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   MIN     -9              8       6       INSERT    NORMAL   UCF      trans_id_2\n"
      "1        var1   MIN     -8              8       5       DELETE    NORMAL   UC       trans_id_2\n";

  micro_data2[1] =
    "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1   MAGIC   MAGIC         NOP      NOP      EXIST     NORMAL   LG\n"
      "2        var2   -26     MIN            7       -1       INSERT    INSERT_DELETE   SCF\n"
      "2        var2   -26     DI_VERSION     7       -1       INSERT    NORMAL   C\n"
      "2        var2   -26     0             -1       -1       DELETE    NORMAL   CL\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_one_macro(&micro_data2[1], 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  for (int64_t i = 1; i < 3; i++) {
    ObTxData *tx_data = new ObTxData();
    transaction::ObTransID tx_id = i;

    // fill in data
    tx_data->tx_id_ = tx_id;
    tx_data->commit_version_.convert_for_tx(i * 20 + 9);
    tx_data->start_scn_.convert_for_tx(i);
    tx_data->end_scn_ = tx_data->commit_version_;
    tx_data->state_ = ObTxData::COMMIT;
    ASSERT_EQ(OB_SUCCESS, tx_table->insert(tx_data));
    delete tx_data;
  }

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 18;
  trans_version_range.base_version_ = 18;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
    "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1   -49     MIN            8        6       INSERT    INSERT_DELETE   SCF\n"
      "1        var1   -49     DI_VERSION     8        6       INSERT    NORMAL   C\n"
      "1        var1   -49     0              8        5       DELETE    NORMAL   C\n"
      "1        var1   -29     DI_VERSION     8        5       INSERT    NORMAL   C\n"
      "1        var1   -29     0              9        5       DELETE    NORMAL   C\n"
      "1        var1   -17     DI_VERSION     9        5       INSERT    NORMAL   CL\n"
      "2        var2   -26     MIN            7       -1       INSERT    INSERT_DELETE   SCF\n"
      "2        var2   -26     DI_VERSION     7       -1       INSERT    NORMAL   C\n"
      "2        var2   -26     0             -1       -1       DELETE    NORMAL   CL\n"
      "3        var3   -16     DI_VERSION    18       -1       INSERT    NORMAL   CLF\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, test_running_trans_cross_macro_with_abort_sql_seq)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN     -12               8      -1       INSERT    NORMAL   UCF trans_id_1\n"
      "1        var1  MIN     -11              -1      -1       DELETE    NORMAL   UC trans_id_1\n";

  micro_data[1] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN     -8                8      -1       INSERT    NORMAL   UC trans_id_1\n"
      "1        var1  MIN     -7               -1      -1       DELETE    NORMAL   UCL trans_id_1\n"
      "3        var3  -16      MIN             18      -1       INSERT    INSERT_DELETE   SCF trans_id_0\n"
      "3        var3  -16      DI_VERSION      18      -1       INSERT    NORMAL   C  trans_id_0\n"
      "3        var3  -16      0               -1      -1       DELETE    NORMAL   CL  trans_id_0\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[2];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0   -26      MIN              7      -1       INSERT    INSERT_DELETE   SCF\n"
      "0        var0   -26      DI_VERSION       7      -1       INSERT    NORMAL   C\n"
      "0        var0   -26      0               -1      -1       DELETE    NORMAL   CL\n"
      "2        var2   -26      MIN              7      -1       INSERT    INSERT_DELETE   SCF\n"
      "2        var2   -26      DI_VERSION       7      -1       INSERT    NORMAL   C\n"
      "2        var2   -26      0               -1      -1       DELETE    NORMAL   CL\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  ObTxData *tx_data = new ObTxData();
  ASSERT_EQ(OB_SUCCESS, tx_data->init_tx_op());
  transaction::ObTransID tx_id = 1;

  // fill in data
  tx_data->tx_id_ = tx_id;
  tx_data->commit_version_.convert_for_tx(INT64_MAX);
  tx_data->start_scn_.convert_for_tx(1);
  tx_data->end_scn_.convert_for_tx(50);
  tx_data->state_ = ObTxData::RUNNING;
  transaction::ObUndoAction undo_action(ObTxSEQ(9, 0), ObTxSEQ(1, 0));
  tx_data->add_undo_action(tx_table, undo_action);
  ASSERT_EQ(OB_SUCCESS, tx_table->insert(tx_data));
  tx_data->~ObTxData();
  delete tx_data;

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 18;
  trans_version_range.base_version_ = 18;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag trans_id\n"
      "0        var0   -26      MIN              7      -1       INSERT    INSERT_DELETE   SCF trans_id_0\n"
      "0        var0   -26      DI_VERSION       7      -1       INSERT    NORMAL   C   trans_id_0\n"
      "0        var0   -26      0               -1      -1       DELETE    NORMAL   CL  trans_id_0\n"
      "1        var1  MIN     -12                8      -1       INSERT    NORMAL   UCF trans_id_1\n"
      "1        var1  MIN     -11               -1      -1       DELETE    NORMAL   UC  trans_id_1\n"
      "1        var1   MAGIC   MAGIC            NOP     NOP      EXIST     NORMAL   LG  trans_id_0\n"
      "2        var2  -26      MIN               7      -1       INSERT    INSERT_DELETE   SCF trans_id_0\n"
      "2        var2  -26      DI_VERSION        7      -1       INSERT    NORMAL   C   trans_id_0\n"
      "2        var2  -26      0                -1      -1       DELETE    NORMAL   CL  trans_id_0\n"
      "3        var3  -16      DI_VERSION      18      -1       INSERT    NORMAL   CLF  trans_id_0\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, check_shadow_row_fuse)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag trans_id\n"
      "0        var0  -8        DI_VERSION  -1       1      INSERT    NORMAL   CF     trans_id_0\n"
      "0        var0  -8        0           -1      -1      DELETE    NORMAL   CL     trans_id_0\n";

  micro_data[1] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag trans_id\n"
      "2        var2  MIN      -9            2        2       INSERT      NORMAL   UCF     trans_id_1\n"
      "2        var2  -6       DI_VERSION    3        2       INSERT      NORMAL   C       trans_id_0\n"
      "2        var2  -6       0            -1       -1       DELETE      NORMAL   C       trans_id_0\n"
      "2        var2  MAGIC    MAGIC        NOP     NOP       EXIST       NORMAL   LG      trans_id_0\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "2        var2  -10      DI_VERSION    2        12      INSERT      NORMAL   CF\n"
      "2        var2  -10      0             2        2       DELETE      NORMAL   CL\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  ObTxData *tx_data = new ObTxData();
  ASSERT_EQ(OB_SUCCESS, tx_data->init_tx_op());
  transaction::ObTransID tx_id = 1;

  // fill in data
  tx_data->tx_id_ = tx_id;
  tx_data->commit_version_.convert_for_tx(INT64_MAX);
  tx_data->start_scn_.convert_for_tx(1);
  tx_data->end_scn_.convert_for_tx(50);
  tx_data->state_ = ObTxData::ABORT;
  ASSERT_EQ(OB_SUCCESS, tx_table->insert(tx_data));
  tx_data->~ObTxData();
  delete tx_data;

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0  -8        DI_VERSION  -1       1      INSERT    NORMAL   CF\n"
      "0        var0  -8        0           -1      -1      DELETE    NORMAL   CL\n"
      "2        var2  -10      MIN           3        12      INSERT      INSERT_DELETE   SCF\n"
      "2        var2  -10      DI_VERSION    3        12      INSERT      NORMAL   C\n"
      "2        var2  -10      0             3        2       DELETE      NORMAL   C\n"
      "2        var2  -6       DI_VERSION    3        2       INSERT      NORMAL   C\n"
      "2        var2  -6       0            -1       -1       DELETE      NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, check_mv_start_compact_shadow_multi_sstables)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -25       MIN             20      20      INSERT    INSERT_DELETE   SCF\n"
      "1        var1  -25       DI_VERSION      20      20      INSERT    NORMAL   C\n"
      "1        var1  -25       0               15      15      DELETE    NORMAL   C\n"
      "1        var1  -15       DI_VERSION      15      15      INSERT    NORMAL   C\n"
      "1        var1  -15       0               10      10      DELETE    NORMAL   CL\n";

  micro_data[1] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "2        var2  -25       MIN             20      20      INSERT    INSERT_DELETE   SCF\n"
      "2        var2  -25       DI_VERSION      20      20      INSERT    NORMAL   C\n"
      "2        var2  -25       0               15      15      DELETE    NORMAL   C\n"
      "2        var2  -15       DI_VERSION      15      15      INSERT    NORMAL   C\n"
      "2        var2  -15       0               10      10      DELETE    NORMAL   CL\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 30;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[2];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -45       MIN             30      30      INSERT    INSERT_DELETE   SCF\n"
      "1        var1  -45       DI_VERSION      30      30      INSERT    NORMAL   C\n"
      "1        var1  -45       0               25      25      DELETE    NORMAL   C\n"
      "1        var1  -35       DI_VERSION      25      25      INSERT    NORMAL   C\n"
      "1        var1  -35       0               20      20      DELETE    NORMAL   CL\n";

  micro_data2[1] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "2        var2  -35       MIN             30      30      INSERT    INSERT_DELETE   SCF\n"
      "2        var2  -35       DI_VERSION      30      30      INSERT    NORMAL   C\n"
      "2        var2  -35       0               25      25      DELETE    NORMAL   C\n"
      "2        var2  -30       DI_VERSION      25      25      INSERT    NORMAL   C\n"
      "2        var2  -30       0               20      20      DELETE    NORMAL   CL\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_one_macro(&micro_data2[1], 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 40;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -45       MIN             30      30      INSERT    INSERT_DELETE   SCF\n"
      "1        var1  -45       DI_VERSION      30      30      INSERT    NORMAL   C\n"
      "1        var1  -45       0               25      25      DELETE    NORMAL   C\n"
      "1        var1  -35       DI_VERSION      25      25      INSERT    NORMAL   C\n"
      "1        var1  -35       0               20      20      DELETE    NORMAL   C\n"
      "1        var1  -25       DI_VERSION      20      20      INSERT    NORMAL   C\n"
      "1        var1  -15       0               10      10      DELETE    NORMAL   CL\n"
      "2        var2  -35       MIN             30      30      INSERT    INSERT_DELETE   SCF\n"
      "2        var2  -35       DI_VERSION      30      30      INSERT    NORMAL   C\n"
      "2        var2  -30       0               20      20      DELETE    NORMAL   C\n"
      "2        var2  -25       DI_VERSION      20      20      INSERT    NORMAL   C\n"
      "2        var2  -15       0               10      10      DELETE    NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, check_mv_start_compact_committing_multi_sstables)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
    "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
    "1        var1  MIN       -10             20      20      INSERT    NORMAL   UCF   trans_id_2\n"
    "1        var1  MIN       -9              15      15      DELETE    NORMAL   UC    trans_id_2\n"
    "1        var1  -18       MIN             15      15      INSERT    NORMAL   SC    trans_id_0\n"
    "1        var1  -18       DI_VERSION      15      15      INSERT    NORMAL   C     trans_id_0\n"
    "1        var1  -18       0               10      10      DELETE    NORMAL   C     trans_id_0\n"
    "1        var1  -15       DI_VERSION      10      10      INSERT    NORMAL   C     trans_id_0\n"
    "1        var1  -15       0                5       5      DELETE    NORMAL   CL    trans_id_0\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 20;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(20);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1  MIN       -12             30      30      INSERT    NORMAL   UCF   trans_id_2\n"
      "1        var1  MIN       -11             20      20      DELETE    NORMAL   UCL   trans_id_2\n";

  snapshot_version = 30;
  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(30);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  ObTxData *tx_data = new ObTxData();
  ASSERT_EQ(OB_SUCCESS, tx_data->init_tx_op());
  transaction::ObTransID tx_id = 2;

  // fill in data
  tx_data->tx_id_ = tx_id;
  tx_data->commit_version_.convert_for_tx(25);
  tx_data->start_scn_.convert_for_tx(1);
  tx_data->end_scn_.convert_for_tx(25);
  tx_data->state_ = ObTxData::COMMIT;
  ASSERT_EQ(OB_SUCCESS, tx_table->insert(tx_data));
  tx_data->~ObTxData();
  delete tx_data;

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -25       MIN             30      30      INSERT    INSERT_DELETE   SCF\n"
      "1        var1  -25       DI_VERSION      30      30      INSERT    NORMAL   C\n"
      "1        var1  -25       0               15      15      DELETE    NORMAL   C\n"
      "1        var1  -18       DI_VERSION      15      15      INSERT    NORMAL   C\n"
      "1        var1  -15       0                5       5      DELETE    NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, check_mv_start_compact_complex_env)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  // delete + insert
  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -20       0               15      15      DELETE    NORMAL   CF\n"
      "1        var1  -15       DI_VERSION      15      15      INSERT    NORMAL   CL\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 20;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(20);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  // insert + insert
  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -30       DI_VERSION      30      30      INSERT    NORMAL   CF\n"
      "1        var1  -30       0               25      25      DELETE    NORMAL   C\n"
      "1        var1  -25       DI_VERSION      25      25      INSERT    NORMAL   CL\n";

  snapshot_version = 30;
  scn_range.start_scn_.convert_for_tx(25);
  scn_range.end_scn_.convert_for_tx(30);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  // insert + delete
  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -40       MIN              40      40      INSERT    INSERT_DELETE   SCF\n"
      "1        var1  -40       DI_VERSION       40      40      INSERT    NORMAL   C\n"
      "1        var1  -40       0                35      35      DELETE    NORMAL   C\n"
      "1        var1  -35       DI_VERSION       35      35      INSERT    NORMAL   C\n"
      "1        var1  -35       0                30      30      DELETE    NORMAL   CL\n";

  snapshot_version = 40;
  scn_range.start_scn_.convert_for_tx(35);
  scn_range.end_scn_.convert_for_tx(40);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  prepare_data_end(handle3);
  merge_context.static_param_.tables_handle_.add_table(handle3);
  STORAGE_LOG(INFO, "finish prepare sstable3");

  // delete + delete
  ObTableHandleV2 handle4;
  const char *micro_data4[1];
  micro_data4[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -60       MIN              55      55      DELETE    INSERT_DELETE   SCF\n"
      "1        var1  -60       0                55      55      DELETE    NORMAL   C\n"
      "1        var1  -55       DI_VERSION       50      50      INSERT    NORMAL   C\n"
      "1        var1  -55       0                45      45      DELETE    NORMAL   C\n"
      "1        var1  -50       DI_VERSION       45      45      INSERT    NORMAL   C\n"
      "1        var1  -45       0                40      40      DELETE    NORMAL   CL\n";

  snapshot_version = 55;
  scn_range.start_scn_.convert_for_tx(45);
  scn_range.end_scn_.convert_for_tx(60);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data4, 1);
  prepare_data_end(handle4);
  merge_context.static_param_.tables_handle_.add_table(handle4);
  STORAGE_LOG(INFO, "finish prepare sstable4");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 70;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
    "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
    "1        var1  -60       MIN              55      55      DELETE    INSERT_DELETE   SCF\n"
    "1        var1  -60       0                55      55      DELETE    NORMAL   C\n"
    "1        var1  -45       0                40      40      DELETE    NORMAL   C\n"
    "1        var1  -40       DI_VERSION       40      40      INSERT    NORMAL   C\n"
    "1        var1  -35       0                30      30      DELETE    NORMAL   C\n"
    "1        var1  -30       DI_VERSION       30      30      INSERT    NORMAL   C\n"
    "1        var1  -20       0                15      15      DELETE    NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  handle3.reset();
  handle4.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, check_mv_start_with_base_version)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -20       DI_VERSION      20      20      INSERT    NORMAL   CF\n"
      "1        var1  -15       0               15      15      DELETE    NORMAL   CL\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 20;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(20);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -25       DI_VERSION      25      25      INSERT    NORMAL   CF\n"
      "1        var1  -25       0               20      20      DELETE    NORMAL   CL\n";

  snapshot_version = 30;
  scn_range.start_scn_.convert_for_tx(25);
  scn_range.end_scn_.convert_for_tx(30);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -35       0                25      25      DELETE    NORMAL   CLF\n";

  snapshot_version = 35;
  scn_range.start_scn_.convert_for_tx(35);
  scn_range.end_scn_.convert_for_tx(40);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  prepare_data_end(handle3);
  merge_context.static_param_.tables_handle_.add_table(handle3);
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 30;
  trans_version_range.base_version_ = 18;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
    "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
    "1        var1  -35       MIN             25      25      DELETE    INSERT_DELETE   SCF\n"
    "1        var1  -35       0               25      25      DELETE    NORMAL   C\n"
    "1        var1  -25       DI_VERSION      25      25      INSERT    NORMAL   C\n"
    "1        var1  -25       0               20      20      DELETE    NORMAL   C\n"
    "1        var1  -20       DI_VERSION      20      20      INSERT    NORMAL   CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  handle3.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, uncommit_row_sql_sequence_check_order_error)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1  MIN      -12       20      20      DELETE    NORMAL    FU   trans_id_1\n"
      "1        var1  MIN      -11       20      20      INSERT    NORMAL    LU   trans_id_1\n"
      "2        var2  MIN      -14       20      20      INSERT    NORMAL    ULF  trans_id_1\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 20;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(20);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "2        var2  MIN      -13       20      20      DELETE    NORMAL    ULF  trans_id_1\n";

  snapshot_version = 30;
  scn_range.start_scn_.convert_for_tx(25);
  scn_range.end_scn_.convert_for_tx(30);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  ObTxData *tx_data = new ObTxData();
  ASSERT_EQ(OB_SUCCESS, tx_data->init_tx_op());
  transaction::ObTransID tx_id = 1;
  // fill in data
  tx_data->tx_id_ = tx_id;
  tx_data->commit_version_.convert_for_tx(25);
  tx_data->start_scn_.convert_for_tx(1);
  tx_data->end_scn_.convert_for_tx(25);
  tx_data->state_ = ObTxData::COMMIT;
  ASSERT_EQ(OB_SUCCESS, tx_table->insert(tx_data));
  tx_data->~ObTxData();
  delete tx_data;

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 30;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -25       0           20      20      DELETE    NORMAL   CLF\n"
      "2        var2  -25       0           20      20      DELETE    NORMAL   CLF\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, crossed_range_checkorder_error)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint                 bigint        bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -1745565103366455000     MIN          20      20      INSERT    NORMAL    SCF\n"
      "1        var1  -1745565103366455000     DI_VERSION   20      20      INSERT    NORMAL    C\n"
      "1        var1  -1745565103366455000     0            20      20      DELETE    NORMAL    C\n"
      "1        var1  -1745564984513289000     DI_VERSION   20      20      INSERT    NORMAL    C\n"
      "1        var1  -1745564984513289000     0            20      20      DELETE    NORMAL    C\n"
      "1        var1  -1745564862088425000     DI_VERSION   20      20      INSERT    NORMAL    C\n"
      "1        var1  -1745564862088425000     0            20      20      DELETE    NORMAL    C\n"
      "1        var1  -1745564841050055000     DI_VERSION   20      20      INSERT    NORMAL    CL\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 1745565132272214002;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(1745565132272214002);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint                 bigint        bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -1745565523125912000      MIN         20      20      INSERT    NORMAL    SCF\n"
      "1        var1  -1745565523125912000      DI_VERSION  20      20      INSERT    NORMAL    C\n"
      "1        var1  -1745565439870560000      0           20      20      DELETE    NORMAL    C\n"
      "1        var1  -1745565432452719000      DI_VERSION  20      20      INSERT    NORMAL    C\n"
      "1        var1  -1745565403991954000      0           20      20      DELETE    NORMAL    C\n"
      "1        var1  -1745565304482379000      DI_VERSION  20      20      INSERT    NORMAL    C\n"
      "1        var1  -1745565304482379000      0           20      20      DELETE    NORMAL    C\n"
      "1        var1  -1745565296623304000      DI_VERSION  20      20      INSERT    NORMAL    C\n"
      "1        var1  -1745565296623304000      0           20      20      DELETE    NORMAL    C\n"
      "1        var1  -1745565239306510002      DI_VERSION  20      20      INSERT    NORMAL    C\n"
      "1        var1  -1745565239306510002      0           20      20      DELETE    NORMAL    CL\n";

  snapshot_version = 1745565526899601005;
  scn_range.start_scn_.convert_for_tx(1745565132272214002);
  scn_range.end_scn_.convert_for_tx(1745565526899601005);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   var   bigint                 bigint        bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -1745565646412642000      MIN         20      20      DELETE    NORMAL    SCF\n"
      "1        var1  -1745565646412642000      0           20      20      DELETE    NORMAL    C\n"
      "1        var1  -1745565557130273000      DI_VERSION  20      20      INSERT    NORMAL    C\n"
      "1        var1  -1745565557130273000      0           20      20      DELETE    NORMAL    C\n"
      "1        var1  -1745565523125912000      DI_VERSION  20      20      INSERT    NORMAL    C\n"
      "1        var1  -1745565239306510002      0           20      20      DELETE    NORMAL    CL\n";

  snapshot_version = 1745566502778393003;
  scn_range.start_scn_.convert_for_tx(1745565526899601005);
  scn_range.end_scn_.convert_for_tx(1745566502778393003);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  prepare_data_end(handle3);
  merge_context.static_param_.tables_handle_.add_table(handle3);
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 1745566502778393003;
  trans_version_range.multi_version_start_ = 1745565771840454000;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -1745565646412642000      MIN         20      20      DELETE    NORMAL    SCF\n"
      "1        var1  -1745565646412642000      0           20      20      DELETE    NORMAL    C\n"
      "1        var1  -1745565523125912000     DI_VERSION   20      20      INSERT    NORMAL    C\n"
      "1        var1  -1745565239306510002      0           20      20      DELETE    NORMAL    C\n"
      "1        var1  -1745565103366455000     DI_VERSION   20      20      INSERT    NORMAL    CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  handle3.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, crossed_range_sstable_merge)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  // 1. new sstable output insert+delete, old sstable output insert+delete
  // 2. new sstable output insert+delete, old sstable output insert
  // 3. new sstable output insert+delete, old sstable output delete
  // 4. new sstable output insert+delete, old sstable output delete+delete
  // 5. new sstable output insert, old sstable output insert+delete
  // 6. new sstable output insert, old sstable output insert
  // 7. new sstable output insert, old sstable output delete
  // 8. new sstable output insert, old sstable output delete+delete
  // 9. new sstable output delete+delete, old sstable output insert+delete
  // 10. new sstable output delete+delete, old sstable output insert
  // 11. new sstable output delete+delete, old sstable output delete
  // 12. new sstable output delete+delete, old sstable output delete+delete
  // 13. new sstable output delete, old sstable output insert+delete
  // 14. new sstable output delete, old sstable output insert
  // 15. new sstable output delete, old sstable output delete
  // 16. new sstable output delete, old sstable output delete+delete
  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint    bigint        bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -20      DI_VERSION          20      20      INSERT    NORMAL    CF\n"
      "1        var1  -10      0                   20      20      DELETE    NORMAL    CL\n"
      "2        var2  -20      DI_VERSION          20      20      INSERT    NORMAL    CLF\n"
      "3        var3  -30      0                   20      20      DELETE    NORMAL    CLF\n"
      "4        var4  -30      0                   20      20      DELETE    NORMAL    CF\n"
      "4        var4  -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "4        var4  -10      0                   20      20      DELETE    NORMAL    CL\n"
      "5        var5  -20      DI_VERSION          20      20      INSERT    NORMAL    CF\n"
      "5        var5  -10      0                   20      20      DELETE    NORMAL    CL\n"
      "6        var6  -20      DI_VERSION          20      20      INSERT    NORMAL    CLF\n"
      "7        var7  -30      0                   20      20      DELETE    NORMAL    CLF\n"
      "8        var8  -30      0                   20      20      DELETE    NORMAL    CF\n"
      "8        var8  -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "8        var8  -10      0                   20      20      DELETE    NORMAL    CL\n"
      "9        var9  -20      DI_VERSION          20      20      INSERT    NORMAL    CF\n"
      "9        var9  -10      0                   20      20      DELETE    NORMAL    CL\n"
      "10       var10 -20      DI_VERSION          20      20      INSERT    NORMAL    CLF\n"
      "11       var11 -30      0                   20      20      DELETE    NORMAL    CLF\n"
      "12       var12 -30      0                   20      20      DELETE    NORMAL    CF\n"
      "12       var12 -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "12       var12 -10      DI_VERSION          20      20      DELETE    NORMAL    CL\n"
      "13       var13 -20      DI_VERSION          20      20      INSERT    NORMAL    CF\n"
      "13       var13 -10      0                   20      20      DELETE    NORMAL    CL\n"
      "14       var14 -20      DI_VERSION          20      20      INSERT    NORMAL    CLF\n"
      "15       var15 -30      0                   20      20      DELETE    NORMAL    CLF\n"
      "16       var16 -30      0                   20      20      DELETE    NORMAL    CF\n"
      "16       var16 -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "16       var16 -10      0                   20      20      DELETE    NORMAL    CL\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 10;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint    bigint        bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -40      DI_VERSION          20      20      INSERT    NORMAL    CF\n"
      "1        var1  -30      0                   20      20      DELETE    NORMAL    C\n"
      "1        var1  -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "1        var1  -10      0                   20      20      DELETE    NORMAL    CL\n"
      "2        var2  -40      DI_VERSION          20      20      INSERT    NORMAL    CF\n"
      "2        var2  -30      0                   20      20      DELETE    NORMAL    C\n"
      "2        var2  -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "2        var2  -10      0                   20      20      DELETE    NORMAL    CL\n"
      "3        var3  -40      DI_VERSION          20      20      INSERT    NORMAL    CF\n"
      "3        var3  -30      0                   20      20      DELETE    NORMAL    C\n"
      "3        var3  -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "3        var3  -10      0                   20      20      DELETE    NORMAL    CL\n"
      "4        var4  -40      DI_VERSION          20      20      INSERT    NORMAL    CF\n"
      "4        var4  -30      0                   20      20      DELETE    NORMAL    C\n"
      "4        var4  -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "4        var4  -10      0                   20      20      DELETE    NORMAL    CL\n"
      "5        var5  -40      DI_VERSION          20      20      INSERT    NORMAL    CF\n"
      "5        var5  -30      0                   20      20      DELETE    NORMAL    C\n"
      "5        var5  -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "5        var5  -10      0                   20      20      DELETE    NORMAL    C\n"
      "5        var5  -5       0                   20      20      INSERT    NORMAL    CL\n"
      "6        var6  -40      DI_VERSION          20      20      INSERT    NORMAL    CF\n"
      "6        var6  -30      0                   20      20      DELETE    NORMAL    C\n"
      "6        var6  -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "6        var6  -10      0                   20      20      DELETE    NORMAL    C\n"
      "6        var6  -5       0                   20      20      INSERT    NORMAL    CL\n"
      "7        var7  -40      DI_VERSION          20      20      INSERT    NORMAL    CF\n"
      "7        var7  -30      0                   20      20      DELETE    NORMAL    C\n"
      "7        var7  -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "7        var7  -10      0                   20      20      DELETE    NORMAL    C\n"
      "7        var7  -5       0                   20      20      INSERT    NORMAL    CL\n"
      "8        var8  -40      DI_VERSION          20      20      INSERT    NORMAL    CF\n"
      "8        var8  -30      0                   20      20      DELETE    NORMAL    C\n"
      "8        var8  -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "8        var8  -10      0                   20      20      DELETE    NORMAL    C\n"
      "8        var8  -5       0                   20      20      INSERT    NORMAL    CL\n"
      "9        var9  -50      0                   20      20      DELETE    NORMAL    CF\n"
      "9        var9  -40      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "9        var9  -30      0                   20      20      DELETE    NORMAL    C\n"
      "9        var9  -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "9        var9  -10      0                   20      20      DELETE    NORMAL    CL\n"
      "10       var10 -50      0                   20      20      DELETE    NORMAL    CF\n"
      "10       var10 -40      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "10       var10 -30      0                   20      20      DELETE    NORMAL    C\n"
      "10       var10 -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "10       var10 -10      0                   20      20      DELETE    NORMAL    CL\n"
      "11       var11 -50      0                   20      20      DELETE    NORMAL    CF\n"
      "11       var11 -40      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "11       var11 -30      0                   20      20      DELETE    NORMAL    C\n"
      "11       var11 -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "11       var11 -10      0                   20      20      DELETE    NORMAL    CL\n"
      "12       var12 -50      0                   20      20      DELETE    NORMAL    CF\n"
      "12       var12 -40      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "12       var12 -30      0                   20      20      DELETE    NORMAL    C\n"
      "12       var12 -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "12       var12 -10      0                   20      20      DELETE    NORMAL    CL\n"
      "13       var13 -50      0                   20      20      DELETE    NORMAL    CF\n"
      "13       var13 -40      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "13       var13 -30      0                   20      20      DELETE    NORMAL    C\n"
      "13       var13 -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "13       var13 -10      0                   20      20      DELETE    NORMAL    C\n"
      "13       var13 -5       DI_VERSION          20      20      INSERT    NORMAL    CL\n"
      "14       var14 -50      0                   20      20      DELETE    NORMAL    CF\n"
      "14       var14 -40      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "14       var14 -30      0                   20      20      DELETE    NORMAL    C\n"
      "14       var14 -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "14       var14 -10      0                   20      20      DELETE    NORMAL    C\n"
      "14       var14 -5       DI_VERSION          20      20      INSERT    NORMAL    CL\n"
      "15       var15 -50      0                   20      20      DELETE    NORMAL    CF\n"
      "15       var15 -40      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "15       var15 -30      0                   20      20      DELETE    NORMAL    C\n"
      "15       var15 -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "15       var15 -10      0                   20      20      DELETE    NORMAL    C\n"
      "15       var15 -5       DI_VERSION          20      20      INSERT    NORMAL    CL\n"
      "16       var16 -50      0                   20      20      DELETE    NORMAL    CF\n"
      "16       var16 -40      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "16       var16 -30      0                   20      20      DELETE    NORMAL    C\n"
      "16       var16 -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "16       var16 -10      0                   20      20      DELETE    NORMAL    C\n"
      "16       var16 -5       DI_VERSION          20      20      INSERT    NORMAL    CL\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 90;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -40      MIN                 20      20      INSERT    NORMAL    SCF\n"
      "1        var1  -40      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "1        var1  -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "1        var1  -10      0                   20      20      DELETE    NORMAL    CL\n"
      "2        var2  -40      MIN                 20      20      INSERT    NORMAL    SCF\n"
      "2        var2  -40      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "2        var2  -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "2        var2  -10      0                   20      20      DELETE    NORMAL    CL\n"
      "3        var3  -40      MIN                 20      20      INSERT    NORMAL    SCF\n"
      "3        var3  -40      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "3        var3  -30      0                   20      20      DELETE    NORMAL    C\n"
      "3        var3  -10      0                   20      20      DELETE    NORMAL    CL\n"
      "4        var4  -40      MIN                 20      20      INSERT    NORMAL    SCF\n"
      "4        var4  -40      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "4        var4  -30      0                   20      20      DELETE    NORMAL    C\n"
      "4        var4  -10      0                   20      20      DELETE    NORMAL    CL\n"
      "5        var5  -40      MIN                 20      20      INSERT    NORMAL    SCF\n"
      "5        var5  -40      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "5        var5  -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "5        var5  -10      0                   20      20      DELETE    NORMAL    CL\n"
      "6        var6  -40      MIN                 20      20      INSERT    NORMAL    SCF\n"
      "6        var6  -40      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "6        var6  -20      DI_VERSION          20      20      INSERT    NORMAL    CL\n"
      "7        var7  -40      MIN                 20      20      INSERT    NORMAL    SCF\n"
      "7        var7  -40      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "7        var7  -30      0                   20      20      DELETE    NORMAL    CL\n"
      "8        var8  -40      MIN                 20      20      INSERT    NORMAL    SCF\n"
      "8        var8  -40      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "8        var8  -30      0                   20      20      DELETE    NORMAL    C\n"
      "8        var8  -10      0                   20      20      DELETE    NORMAL    CL\n"
      "9        var9  -50      MIN                 20      20      DELETE    NORMAL    SCF\n"
      "9        var9  -50      0                   20      20      DELETE    NORMAL    C\n"
      "9        var9  -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "9        var9  -10      0                   20      20      DELETE    NORMAL    CL\n"
      "10       var10 -50      MIN                 20      20      DELETE    NORMAL    SCF\n"
      "10       var10 -50      0                   20      20      DELETE    NORMAL    C\n"
      "10       var10 -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "10       var10 -10      0                   20      20      DELETE    NORMAL    CL\n"
      "11       var11 -50      MIN                 20      20      DELETE    NORMAL    SCF\n"
      "11       var11 -50      0                   20      20      DELETE    NORMAL    C\n"
      "11       var11 -30      0                   20      20      DELETE    NORMAL    C\n"
      "11       var11 -10      0                   20      20      DELETE    NORMAL    CL\n"
      "12       var12 -50      MIN                 20      20      DELETE    NORMAL    SCF\n"
      "12       var12 -50      0                   20      20      DELETE    NORMAL    C\n"
      "12       var12 -30      0                   20      20      DELETE    NORMAL    C\n"
      "12       var12 -10      0                   20      20      DELETE    NORMAL    CL\n"
      "13       var13 -50      MIN                 20      20      DELETE    NORMAL    SCF\n"
      "13       var13 -50      0                   20      20      DELETE    NORMAL    C\n"
      "13       var13 -20      DI_VERSION          20      20      INSERT    NORMAL    C\n"
      "13       var13 -10      0                   20      20      DELETE    NORMAL    CL\n"
      "14       var14 -50      MIN                 20      20      DELETE    NORMAL    SCF\n"
      "14       var14 -50      0                   20      20      DELETE    NORMAL    C\n"
      "14       var14 -20      DI_VERSION          20      20      INSERT    NORMAL    CL\n"
      "15       var15 -50      MIN                 20      20      DELETE    NORMAL    SCF\n"
      "15       var15 -50      0                   20      20      DELETE    NORMAL    C\n"
      "15       var15 -30      0                   20      20      DELETE    NORMAL    CL\n"
      "16       var16 -50      MIN                 20      20      DELETE    NORMAL    SCF\n"
      "16       var16 -50      0                   20      20      DELETE    NORMAL    C\n"
      "16       var16 -30      0                   20      20      DELETE    NORMAL    C\n"
      "16       var16 -10      0                   20      20      DELETE    NORMAL    CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, compact_old_row_check_order_error)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint                 bigint        bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -1745818547933984005     DI_VERSION   20      20      INSERT    NORMAL    CLF\n"
      "3        var3  -1745818547933984005     DI_VERSION   20      20      INSERT    NORMAL    CLF\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 1745818547933984005;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(1745818547933984005);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint                 bigint        bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "2        var2  -1745818788284569004      MIN         20      20      DELETE    NORMAL    SCF\n"
      "2        var2  -1745818788284569004      0           20      20      DELETE    NORMAL    C\n"
      "2        var2  -1745818547933984005      DI_VERSION  20      20      INSERT    NORMAL    C\n"
      "2        var2  -1745818547933984005      0           20      20      DELETE    NORMAL    CL\n";

  snapshot_version = 1745565526899601005;
  scn_range.start_scn_.convert_for_tx(1745818547933984005);
  scn_range.end_scn_.convert_for_tx(1745818788284569004);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 1745820760811606007;
  trans_version_range.multi_version_start_ = 1745820062414483000;
  trans_version_range.base_version_ = 1745818585511053007;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -1745818547933984005      DI_VERSION   20      20      INSERT    NORMAL    CLF\n"
      "2        var2  -1745818788284569004      0           20      20      DELETE    NORMAL    CLF\n"
      "3        var3  -1745818547933984005      DI_VERSION  20      20      INSERT    NORMAL    CLF\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, compact_old_row_with_base_version)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint       bigint        bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1  -40          DI_VERSION   20      20      INSERT    NORMAL    CF  trans_id_0\n"
      "1        var1  -30          0            20      20      DELETE    NORMAL    C   trans_id_0\n"
      "1        var1  -20          DI_VERSION   20      20      INSERT    NORMAL    C   trans_id_0\n"
      "1        var1  -10          0            20      20      DELETE    NORMAL    C   trans_id_0\n"
      "1        var1  -5           DI_VERSION   20      20      INSERT    NORMAL    CL  trans_id_0\n"
      "3        var3  -40          0            20      20      DELETE    NORMAL    CF  trans_id_0\n"
      "3        var3  -30          DI_VERSION   20      20      INSERT    NORMAL    C   trans_id_0\n"
      "3        var3  -20          0            20      20      DELETE    NORMAL    C   trans_id_0\n"
      "3        var3  -10          DI_VERSION   20      20      INSERT    NORMAL    C   trans_id_0\n"
      "3        var3  -5           0            20      20      DELETE    NORMAL    CL  trans_id_0\n"
      "5        var5  -4           0            20      20      DELETE    NORMAL    CF  trans_id_0\n"
      "5        var5  -3           DI_VERSION   20      20      INSERT    NORMAL    C   trans_id_0\n"
      "5        var5  -2           0            20      20      DELETE    NORMAL    C   trans_id_0\n"
      "5        var5  -1           DI_VERSION   20      20      INSERT    NORMAL    CL  trans_id_0\n"
      "6        var6  -3           DI_VERSION   20      20      INSERT    NORMAL    CF  trans_id_0\n"
      "6        var6  -2           0            20      20      DELETE    NORMAL    C   trans_id_0\n"
      "6        var6  -1           DI_VERSION   20      20      INSERT    NORMAL    CL  trans_id_0\n"
      "7        var7  MIN          -9           20      20      DELETE    NORMAL    UCF  trans_id_1\n"
      "7        var7  MIN          -8           20      20      INSERT    NORMAL    UC   trans_id_1\n"
      "7        var7  MIN          -7           20      20      DELETE    NORMAL    UCL  trans_id_1\n"
      "8        var8  -40          DI_VERSION   20      20      INSERT    NORMAL    CF  trans_id_0\n"
      "8        var8  -40          0            20      20      DELETE    NORMAL    CL  trans_id_0\n";
  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 25;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(25);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint       bigint        bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "2        var2  -30          0            20      20      DELETE    NORMAL    CF\n"
      "2        var2  -20          DI_VERSION   20      20      INSERT    NORMAL    C\n"
      "2        var2  -10          0            20      20      DELETE    NORMAL    C\n"
      "2        var2  -5           DI_VERSION   20      20      INSERT    NORMAL    CL\n"
      "4        var4  -50          DI_VERSION   20      20      INSERT    NORMAL    CF\n"
      "4        var4  -40          0            20      20      DELETE    NORMAL    C\n"
      "4        var4  -30          DI_VERSION   20      20      INSERT    NORMAL    C\n"
      "4        var4  -20          0            20      20      DELETE    NORMAL    C\n"
      "4        var4  -10          DI_VERSION   20      20      INSERT    NORMAL    C\n"
      "4        var4  -5           0            20      20      DELETE    NORMAL    CL\n"
      "5        var5  -40          0            20      20      DELETE    NORMAL    CF\n"
      "5        var5  -30          DI_VERSION   20      20      INSERT    NORMAL    C\n"
      "5        var5  -20          0            20      20      DELETE    NORMAL    C\n"
      "5        var5  -10          DI_VERSION   20      20      INSERT    NORMAL    CL\n"
      "6        var6  -30          DI_VERSION   20      20      INSERT    NORMAL    CF\n"
      "6        var6  -20          0            20      20      DELETE    NORMAL    CL\n";
  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(25);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  for (int64_t i = 1; i <= 1; i++) {
    ObTxData *tx_data = new ObTxData();
    transaction::ObTransID tx_id = i;

    // fill in data
    tx_data->tx_id_ = tx_id;
    tx_data->commit_version_.convert_for_tx(i * 10);
    tx_data->start_scn_.convert_for_tx(i);
    tx_data->end_scn_ = tx_data->commit_version_;
    tx_data->state_ = ObTxData::COMMIT;

    ASSERT_EQ(OB_SUCCESS, tx_table->insert(tx_data));
    delete tx_data;
  }


  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 80;
  trans_version_range.base_version_ = 9;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "1        var1  -40          DI_VERSION   20      20      INSERT    NORMAL    CF\n"
      "1        var1  -10          0            20      20      DELETE    NORMAL    CL\n"
      "2        var2  -30          0            20      20      DELETE    NORMAL    CF\n"
      "2        var2  -10          0            20      20      DELETE    NORMAL    CL\n"
      "3        var3  -40          0            20      20      DELETE    NORMAL    CLF\n"
      "4        var4  -50          DI_VERSION   20      20      INSERT    NORMAL    CLF\n"
      "5        var5  -40          MIN          20      20      DELETE    NORMAL    SCF\n"
      "5        var5  -40          0            20      20      DELETE    NORMAL    C\n"
      "5        var5  -4           0            20      20      DELETE    NORMAL    CL\n"
      "6        var6  -30          MIN          20      20      INSERT    NORMAL    SCF\n"
      "6        var6  -30          DI_VERSION   20      20      INSERT    NORMAL    C\n"
      "6        var6  -20          0            20      20      DELETE    NORMAL    C\n"
      "6        var6  -3           DI_VERSION   20      20      INSERT    NORMAL    CL\n"
      "7        var7  -10          0            20      20      DELETE    NORMAL    CLF\n"
      "8        var8  -40          DI_VERSION   20      20      INSERT    NORMAL    CF\n"
      "8        var8  -40          0            20      20      DELETE    NORMAL    CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  clear_tx_data();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, single_trans_replayed_in_multi_sst)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[5];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0   -9       MIN           7        12      INSERT    INSERT_DELETE   SCF\n"
      "0        var0   -9       DI_VERSION    7        12      INSERT    NORMAL   C\n"
      "0        var0   -9       0             -1       -1      DELETE    NORMAL   CL\n";

  micro_data[1] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   -44      MIN           7        59      INSERT    INSERT_DELETE   SCF   trans_id_0\n"
      "1        var1   -44      DI_VERSION    7        59      INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -44      0             7        28      DELETE    NORMAL   C     trans_id_0\n";

  micro_data[2] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   -33      DI_VERSION    7        28      INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -33      0             7        71      DELETE    NORMAL   C     trans_id_0\n";

  micro_data[3] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   -22      DI_VERSION    7        71      INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -22      0             7        2       DELETE    NORMAL   C     trans_id_0\n";

  micro_data[4] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   -11      DI_VERSION    7        2       INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -11      0            -1        9       DELETE    NORMAL   C     trans_id_0\n"
      "1        var1   -4       DI_VERSION   -1        9       INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -4       0            -1       -1       DELETE    NORMAL   CL    trans_id_0\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1, INT64_MAX, true);
  prepare_one_macro(&micro_data[2], 1, INT64_MAX, true);
  prepare_one_macro(&micro_data[3], 1, INT64_MAX, true);
  prepare_one_macro(&micro_data[4], 1, INT64_MAX, true);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "0        var0  -9        MIN           9        12      INSERT    INSERT_DELETE   SCF   trans_id_0\n"
      "0        var0  -9        DI_VERSION    9        12      INSERT    NORMAL   C     trans_id_0\n"
      "0        var0  -9        0             7        12      DELETE    NORMAL   CL    trans_id_0\n"
      "1        var1  MIN       -89           7        9       INSERT    NORMAL   UCF   trans_id_5\n"
      "1        var1  MIN       -88           7        100     DELETE    NORMAL   UC    trans_id_5\n"
      "1        var1  -44       MIN           7        100     INSERT    INSERT_DELETE   SC    trans_id_0\n"
      "1        var1  -44       DI_VERSION    7        100     INSERT    NORMAL   C     trans_id_0\n"
      "1        var1  -44       0             7        59      DELETE    NORMAL   CL    trans_id_0\n"
      "2        var2  MIN       -15          12        7       INSERT    NORMAL   UCF   trans_id_1\n"
      "2        var2  MIN       -14          -1        7       DELETE    NORMAL   UC    trans_id_1\n"
      "2        var2   -4       MIN          -1        7       INSERT    INSERT_DELETE   SC    trans_id_0\n"
      "2        var2   -4       DI_VERSION   -1        7       INSERT    NORMAL   C     trans_id_0\n"
      "2        var2   -4       0            -1       -1       DELETE    NORMAL   CL    trans_id_0\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1, INT64_MAX, true);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1  MIN       -89           7        9       INSERT    NORMAL   UCF   trans_id_5\n"
      "1        var1  MIN       -88           7        100     DELETE    NORMAL   UCL    trans_id_5\n"
      "2        var2  MIN       -25          18        7       INSERT    NORMAL   UCF    trans_id_1\n"
      "2        var2  MIN       -24          12        7       DELETE    NORMAL   UCL    trans_id_1\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(100);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  prepare_data_end(handle3);
  merge_context.static_param_.tables_handle_.add_table(handle3);
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  for (int64_t i = 1; i <= 5; i++) {
    ObTxData *tx_data = new ObTxData();
    ASSERT_EQ(OB_SUCCESS, tx_data->init_tx_op());
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
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "0        var0  -9        MIN           9        12      INSERT    INSERT_DELETE   SCF   trans_id_0\n"
      "0        var0  -9        DI_VERSION    9        12      INSERT    NORMAL   C     trans_id_0\n"
      "0        var0   -9       0             -1       -1      DELETE    NORMAL   CL    trans_id_0\n"
      "1        var1  MIN       -89           7        9       INSERT    NORMAL   UCF   trans_id_5\n"
      "1        var1  MIN       -88           7        100     DELETE    NORMAL   UC    trans_id_5\n"
      "1        var1  -44       MIN           7        100     INSERT    INSERT_DELETE   SC    trans_id_0\n"
      "1        var1  -44       DI_VERSION    7        100     INSERT    NORMAL   C     trans_id_0\n"
      "1        var1  -44       0             7        28      DELETE    NORMAL   C     trans_id_0\n"
      "1        var1   -33      DI_VERSION    7        28      INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -33      0             7        71      DELETE    NORMAL   C     trans_id_0\n"
      "1        var1   -22      DI_VERSION    7        71      INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -22      0             7        2       DELETE    NORMAL   C     trans_id_0\n"
      "1        var1   -11      DI_VERSION    7        2       INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -11      0            -1        9       DELETE    NORMAL   C     trans_id_0\n"
      "1        var1   -4       DI_VERSION   -1        9       INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -4       0            -1       -1       DELETE    NORMAL   CL    trans_id_0\n"
      "2        var2  -11       MIN          18        7       INSERT    INSERT_DELETE   SCF   trans_id_0\n"
      "2        var2  -11       DI_VERSION   18        7       INSERT    NORMAL   C     trans_id_0\n"
      "2        var2  -11       0            -1        7       DELETE    NORMAL   C     trans_id_0\n"
      "2        var2   -4       DI_VERSION   -1        7       INSERT    NORMAL   C     trans_id_0\n"
      "2        var2   -4       0            -1       -1       DELETE    NORMAL   CL    trans_id_0\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  handle3.reset();
  merger.reset();
}

TEST_F(TestMultiVersionDIMerge, lock_row_replayed_in_multi_sst)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[5];
  micro_data[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag\n"
      "0        var0   -9       MIN           7        12      INSERT    INSERT_DELETE   SCF\n"
      "0        var0   -9       DI_VERSION    7        12      INSERT    NORMAL   C\n"
      "0        var0   -9       0             -1       -1      DELETE    NORMAL   CL\n";

  micro_data[1] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   -44      MIN           7        59      INSERT    INSERT_DELETE   SCF   trans_id_0\n"
      "1        var1   -44      DI_VERSION    7        59      INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -44      0             7        28      DELETE    NORMAL   C     trans_id_0\n";

  micro_data[2] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   -33      DI_VERSION    7        28      INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -33      0             7        71      DELETE    NORMAL   C     trans_id_0\n";

  micro_data[3] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   -22      DI_VERSION    7        71      INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -22      0             7        2       DELETE    NORMAL   C     trans_id_0\n";

  micro_data[4] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1   -11      DI_VERSION    7        2       INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -11      0            -1        9       DELETE    NORMAL   C     trans_id_0\n"
      "1        var1   -4       DI_VERSION   -1        9       INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -4       0            -1       -1       DELETE    NORMAL   CL    trans_id_0\n"
      "3        var3   MIN      -99           7        12      INSERT    NORMAL   UCF   trans_id_5\n"
      "3        var3   MIN      -98           -1       -1      DELETE    NORMAL   UC    trans_id_5\n"
      "3        var3   MIN      -97          NOP       NOP     LOCK      NORMAL   LU    trans_id_5\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1, INT64_MAX, true);
  prepare_one_macro(&micro_data[2], 1, INT64_MAX, true);
  prepare_one_macro(&micro_data[3], 1, INT64_MAX, true);
  prepare_one_macro(&micro_data[4], 1, INT64_MAX, true);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "0        var0  -9        MIN           9        12      INSERT    INSERT_DELETE   SCF   trans_id_0\n"
      "0        var0  -9        DI_VERSION    9        12      INSERT    NORMAL   C     trans_id_0\n"
      "0        var0  -9        0             7        12      DELETE    NORMAL   CL    trans_id_0\n"
      "1        var1  MIN       -89           7        9       INSERT    NORMAL   UCF   trans_id_5\n"
      "1        var1  MIN       -88           7        100     DELETE    NORMAL   UC    trans_id_5\n"
      "1        var1  -44       MIN           7        100     INSERT    INSERT_DELETE   SC    trans_id_0\n"
      "1        var1  -44       DI_VERSION    7        100     INSERT    NORMAL   C     trans_id_0\n"
      "1        var1  -44       0             7        59      DELETE    NORMAL   CL    trans_id_0\n"
      "2        var2  MIN       -15          12        7       INSERT    NORMAL   UCF   trans_id_1\n"
      "2        var2  MIN       -14          -1        7       DELETE    NORMAL   UC    trans_id_1\n"
      "2        var2   -4       MIN          -1        7       INSERT    INSERT_DELETE   SC    trans_id_0\n"
      "2        var2   -4       DI_VERSION   -1        7       INSERT    NORMAL   C     trans_id_0\n"
      "2        var2   -4       0            -1       -1       DELETE    NORMAL   CL    trans_id_0\n";

  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1, INT64_MAX, true);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "1        var1  MIN       -89           7        9       INSERT    NORMAL   UCF   trans_id_5\n"
      "1        var1  MIN       -88           7        100     DELETE    NORMAL   UCL    trans_id_5\n"
      "2        var2  MIN       -25          18        7       INSERT    NORMAL   UCF    trans_id_1\n"
      "2        var2  MIN       -24          12        7       DELETE    NORMAL   UCL    trans_id_1\n"
      "3        var3   MIN      -99           7        12      INSERT    NORMAL   UCF   trans_id_5\n"
      "3        var3   MIN      -98           -1       -1      DELETE    NORMAL   UC    trans_id_5\n"
      "3        var3   MIN      -97          NOP       NOP     LOCK      NORMAL   LU    trans_id_5\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(100);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  prepare_data_end(handle3);
  merge_context.static_param_.tables_handle_.add_table(handle3);
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  for (int64_t i = 1; i <= 5; i++) {
    ObTxData *tx_data = new ObTxData();
    ASSERT_EQ(OB_SUCCESS, tx_data->init_tx_op());
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
    }

    ASSERT_EQ(OB_SUCCESS, tx_table->insert(tx_data));
    delete tx_data;
  }

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint     bigint bigint   flag    flag_type  multi_version_row_flag  trans_id\n"
      "0        var0  -9        MIN           9        12      INSERT    INSERT_DELETE   SCF   trans_id_0\n"
      "0        var0  -9        DI_VERSION    9        12      INSERT    NORMAL   C     trans_id_0\n"
      "0        var0   -9       0             -1       -1      DELETE    NORMAL   CL    trans_id_0\n"
      "1        var1  MIN       -89           7        9       INSERT    NORMAL   UCF   trans_id_5\n"
      "1        var1  MIN       -88           7        100     DELETE    NORMAL   UC    trans_id_5\n"
      "1        var1  -44       MIN           7        100     INSERT    INSERT_DELETE   SC    trans_id_0\n"
      "1        var1  -44       DI_VERSION    7        100     INSERT    NORMAL   C     trans_id_0\n"
      "1        var1  -44       0             7        28      DELETE    NORMAL   C     trans_id_0\n"
      "1        var1   -33      DI_VERSION    7        28      INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -33      0             7        71      DELETE    NORMAL   C     trans_id_0\n"
      "1        var1   -22      DI_VERSION    7        71      INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -22      0             7        2       DELETE    NORMAL   C     trans_id_0\n"
      "1        var1   -11      DI_VERSION    7        2       INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -11      0            -1        9       DELETE    NORMAL   C     trans_id_0\n"
      "1        var1   -4       DI_VERSION   -1        9       INSERT    NORMAL   C     trans_id_0\n"
      "1        var1   -4       0            -1       -1       DELETE    NORMAL   CL    trans_id_0\n"
      "2        var2  -11       MIN          18        7       INSERT    INSERT_DELETE   SCF   trans_id_0\n"
      "2        var2  -11       DI_VERSION   18        7       INSERT    NORMAL   C     trans_id_0\n"
      "2        var2  -11       0            -1        7       DELETE    NORMAL   C     trans_id_0\n"
      "2        var2   -4       DI_VERSION   -1        7       INSERT    NORMAL   C     trans_id_0\n"
      "2        var2   -4       0            -1       -1       DELETE    NORMAL   CL    trans_id_0\n"
      "3        var3   MIN      -99           7        12      INSERT    NORMAL   UCF   trans_id_5\n"
      "3        var3   MIN      -98           -1       -1      DELETE    NORMAL   UC    trans_id_5\n"
      "3        var3   MIN      -97          NOP       NOP     LOCK      NORMAL   LU    trans_id_5\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_multi_version_sstable_delete_insert_merge.log*");
  OB_LOGGER.set_file_name("test_multi_version_sstable_delete_insert_merge.log");
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
