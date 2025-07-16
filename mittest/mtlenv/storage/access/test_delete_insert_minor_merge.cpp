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


class TestDeleteInsertMerge : public TestMergeBasic
{
public:
  static const int64_t MAX_PARALLEL_DEGREE = 10;
  TestDeleteInsertMerge();
  virtual ~TestDeleteInsertMerge() {}

  void SetUp();
  void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
  void prepare_query_param(const ObVersionRange &version_range);
  void prepare_txn(ObStoreCtx *store_ctx, const int64_t prepare_version);

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

void TestDeleteInsertMerge::SetUpTestCase()
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

void TestDeleteInsertMerge::TearDownTestCase()
{
  clear_tx_data();
  ObMultiVersionSSTableTest::TearDownTestCase();
  // reset sequence no
  ObClockGenerator::destroy();
}

TestDeleteInsertMerge::TestDeleteInsertMerge()
  : TestMergeBasic("test_delete_insert_minor_merge")
{}

void TestDeleteInsertMerge::SetUp()
{
  ObMultiVersionSSTableTest::SetUp();
}

void TestDeleteInsertMerge::fake_freeze_info()
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

void TestDeleteInsertMerge::TearDown()
{

  ObMultiVersionSSTableTest::TearDown();
  TRANS_LOG(INFO, "teardown success");
}

void TestDeleteInsertMerge::prepare_txn(ObStoreCtx *store_ctx,
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

void TestDeleteInsertMerge::prepare_query_param(const ObVersionRange &version_range)
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

void TestDeleteInsertMerge::prepare_merge_context(const ObMergeType &merge_type,
                                                  const bool is_full_merge,
                                                  const ObVersionRange &trans_version_range,
                                                  ObTabletMergeCtx &merge_context)
{
  TestMergeBasic::prepare_merge_context(merge_type, is_full_merge, trans_version_range, merge_context);
  merge_context.static_param_.for_unittest_ = true;
  merge_context.merge_dag_ = &merge_dag_;
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

void TestDeleteInsertMerge::build_sstable(
    ObTabletMergeCtx &ctx,
    ObSSTable *&merged_sstable)
{
  bool tmp_bool = false; // placeholder
  ASSERT_EQ(OB_SUCCESS, ctx.merge_info_.create_sstable(ctx, ctx.merged_table_handle_, tmp_bool));
  ASSERT_EQ(OB_SUCCESS, ctx.merged_table_handle_.get_sstable(merged_sstable));
}

void TestDeleteInsertMerge::get_tx_table_guard(ObTxTableGuard &tx_table_guard)
{
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tx_table_guard(tx_table_guard));
}

TEST_F(TestDeleteInsertMerge, test_committed_multi_update)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      MIN        9       9     INSERT    NORMAL SCF\n"
      "1        var1  -20      DI_VERSION 9       9     INSERT    NORMAL C\n"
      "1        var1  -20      0          9       1     DELETE    NORMAL        C\n"
      "1        var1  -10      DI_VERSION  9       1     INSERT    NORMAL C\n"
      "1        var1  -10      0          1       1     DELETE    NORMAL        C\n"
      "1        var1  -8       0          1       1     INSERT    NORMAL        CL\n"
      "2        var2  -20      0          2       2     INSERT    NORMAL        CLF\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 30;
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
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        var2  -50      DI_VERSION    9       9     INSERT    NORMAL CF\n"
      "2        var2  -50      0            2       2     DELETE    NORMAL        CL\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   var   bigint  bigint    bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -80      MIN        19     19     INSERT    NORMAL SCF\n"
      "1        var1  -80      DI_VERSION  19     19     INSERT    NORMAL C\n"
      "1        var1  -80      0          19      9     DELETE    NORMAL        C\n"
      "1        var1  -60      DI_VERSION  19      9     INSERT    NORMAL C\n"
      "1        var1  -60      0          9       9     DELETE    NORMAL        CL\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(80);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  prepare_data_end(handle3);
  merge_context.static_param_.tables_handle_.add_table(handle3);
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 3;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -80      MIN          19     19     INSERT    NORMAL SCF\n"
      "1        var1  -80      DI_VERSION    19     19     INSERT    NORMAL C\n"
      "1        var1  -80      0            19      9     DELETE    NORMAL        C\n"
      "1        var1  -60      DI_VERSION    19      9     INSERT    NORMAL C\n"
      "1        var1  -60      0            9       9     DELETE    NORMAL        C\n"
      "1        var1  -20      DI_VERSION    9       9     INSERT    NORMAL C\n"
      "1        var1  -20      0            9       1     DELETE    NORMAL        C\n"
      "1        var1  -10      DI_VERSION    9       1     INSERT    NORMAL C\n"
      "1        var1  -10      0            1       1     DELETE    NORMAL        C\n"
      "1        var1  -8       DI_VERSION   1       1     INSERT    NORMAL        CL\n"
      "2        var2  -50      MIN          9       9     INSERT    NORMAL SCF\n"
      "2        var2  -50      DI_VERSION    9       9     INSERT    NORMAL C\n"
      "2        var2  -50      0            2       2     DELETE    NORMAL        C\n"
      "2        var2  -20      DI_VERSION   2       2     INSERT    NORMAL        CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/, false/*cmp dml row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  handle3.reset();
  merger.reset();
}

// insert1 -> delete2-> insert2 -> delete3-> insert3 ==> insert3
TEST_F(TestDeleteInsertMerge, test_insert_update_one_trans)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -100       9       9     INSERT    NORMAL UCF              trans_id_1\n"
      "1        var1  MIN      -99        9       1     DELETE    NORMAL        UC               trans_id_1\n"
      "1        var1  MIN      -80        9       1     INSERT    NORMAL UC               trans_id_1\n"
      "1        var1  MIN      -79        1       1     DELETE    NORMAL        UC               trans_id_1\n"
      "1        var1  MIN      -10        1       1     INSERT    NORMAL        UCL              trans_id_1\n"
      "2        var2  MIN      -150       2       2     INSERT    NORMAL        UCFL             trans_id_2\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 30;
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
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "2        var2  MIN      -100         19      19    INSERT    NORMAL UCF               trans_id_3\n"
      "2        var2  MIN      -99          9       9     DELETE    NORMAL        UC                trans_id_3\n"
      "2        var2  MIN      -80          9       9     INSERT    NORMAL UC                trans_id_3\n"
      "2        var2  MIN      -79          2       2     DELETE    NORMAL        UCL               trans_id_3\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   var   bigint  bigint    bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -100       19     19     INSERT    NORMAL UCF         trans_id_4\n"
      "1        var1  MIN      -99        9      9      DELETE    NORMAL        UCL         trans_id_4\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(80);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
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

  for (int64_t i = 1; i <= 5; i++) {
    ObTxData *tx_data = new ObTxData();
    ASSERT_EQ(OB_SUCCESS, tx_data->init_tx_op());
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
  trans_version_range.multi_version_start_ = 3;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -40      MIN          19     19     INSERT    NORMAL SCF\n"
      "1        var1  -40      DI_VERSION    19      19    INSERT    NORMAL C\n"
      "1        var1  -40      0            9       9     DELETE    NORMAL        C\n"
      "1        var1  -10      DI_VERSION    9       9     INSERT    NORMAL CL\n"
      "2        var2  -30      MIN          19      19    INSERT    NORMAL SCF\n"
      "2        var2  -30      DI_VERSION    19      19    INSERT    NORMAL C\n"
      "2        var2  -30      0            2       2     DELETE    NORMAL        C\n"
      "2        var2  -20      DI_VERSION   2       2     INSERT    NORMAL        CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/, false/*cmp dml row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  handle3.reset();
  merger.reset();
}

// delete1 -> insert1 -> delete2 -> insert2 -> delete3 -> insert3 ==> delete1 -> insert3
TEST_F(TestDeleteInsertMerge, test_delete_insert_one_trans)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -100       9       9     INSERT    NORMAL UCF              trans_id_1\n"
      "1        var1  MIN      -99        9       1     DELETE    NORMAL        UC               trans_id_1\n"
      "1        var1  MIN      -80        9       1     INSERT    NORMAL UC               trans_id_1\n"
      "1        var1  MIN      -79        1       1     DELETE    NORMAL        UCL              trans_id_1\n"
      "2        var2  MIN      -150       2       2     INSERT    NORMAL        UCFL             trans_id_2\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 30;
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
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "2        var2  MIN      -100         19      19    INSERT    NORMAL UCF               trans_id_3\n"
      "2        var2  MIN      -99          9       9     DELETE    NORMAL        UC                trans_id_3\n"
      "2        var2  MIN      -80          9       9     INSERT    NORMAL UC                trans_id_3\n"
      "2        var2  MIN      -79          2       2     DELETE    NORMAL        UCL               trans_id_3\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   var   bigint  bigint    bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -100       19     19     INSERT    NORMAL UCF         trans_id_4\n"
      "1        var1  MIN      -99        9      9      DELETE    NORMAL        UCL         trans_id_4\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(80);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
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

  for (int64_t i = 1; i <= 5; i++) {
    ObTxData *tx_data = new ObTxData();
    ASSERT_EQ(OB_SUCCESS, tx_data->init_tx_op());
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
  trans_version_range.multi_version_start_ = 3;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -40      MIN          19     19     INSERT    NORMAL  SCF\n"
      "1        var1  -40      DI_VERSION    19      19    INSERT    NORMAL C\n"
      "1        var1  -40      0            9       9     DELETE    NORMAL        C\n"
      "1        var1  -10      DI_VERSION    9       9     INSERT    NORMAL C\n"
      "1        var1  -10      0            1       1     DELETE    NORMAL        CL\n"
      "2        var2  -30      MIN          19      19    INSERT    NORMAL SCF\n"
      "2        var2  -30      DI_VERSION    19      19    INSERT    NORMAL C\n"
      "2        var2  -30      0            2       2     DELETE    NORMAL        C\n"
      "2        var2  -20      DI_VERSION   2       2     INSERT    NORMAL        CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/, false/*cmp dml row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  handle3.reset();
  merger.reset();
}

// delete1 -> insert1 -> delete2 -> insert2 -> delete3 ==> delete1
TEST_F(TestDeleteInsertMerge, test_multi_delete_one_trans)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -100       9       9     INSERT    NORMAL UCF              trans_id_1\n"
      "1        var1  MIN      -99        9       1     DELETE    NORMAL        UC               trans_id_1\n"
      "1        var1  MIN      -80        9       1     INSERT    NORMAL UC               trans_id_1\n"
      "1        var1  MIN      -79        1       1     DELETE    NORMAL        UC               trans_id_1\n"
      "1        var1  MIN      -10        1       1     INSERT    NORMAL        UCL              trans_id_1\n"
      "2        var2  MIN      -150       2       2     INSERT    NORMAL        UCFL             trans_id_2\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 30;
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
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "2        var2  MIN      -100         19      19    INSERT    NORMAL UCF               trans_id_3\n"
      "2        var2  MIN      -99          9       9     DELETE    NORMAL        UC                trans_id_3\n"
      "2        var2  MIN      -80          9       9     INSERT    NORMAL UC                trans_id_3\n"
      "2        var2  MIN      -79          2       2     DELETE    NORMAL        UCL               trans_id_3\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   var   bigint  bigint    bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -130       29     29     DELETE    NORMAL        UCF         trans_id_4\n"
      "1        var1  MIN      -120       29     29     INSERT    NORMAL UC          trans_id_4\n"
      "1        var1  MIN      -119       19     19     DELETE    NORMAL        UC          trans_id_4\n"
      "1        var1  MIN      -100       19     19     INSERT    NORMAL UC          trans_id_4\n"
      "1        var1  MIN      -99        9      9      DELETE    NORMAL        UCL         trans_id_4\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(80);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
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

  for (int64_t i = 1; i <= 5; i++) {
    ObTxData *tx_data = new ObTxData();
    ASSERT_EQ(OB_SUCCESS, tx_data->init_tx_op());
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
  trans_version_range.multi_version_start_ = 3;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -40      MIN          9       9     DELETE    INSERT_DELETE SCF\n"
      "1        var1  -40      0            9       9     DELETE    NORMAL        C\n"
      "1        var1  -10      DI_VERSION    9       9     INSERT    NORMAL CL\n"
      "2        var2  -30      MIN          19      19    INSERT    NORMAL SCF\n"
      "2        var2  -30      DI_VERSION   19      19    INSERT    NORMAL C\n"
      "2        var2  -30      0            2       2     DELETE    NORMAL        C\n"
      "2        var2  -20      DI_VERSION   2       2     INSERT    NORMAL        CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/, false/*cmp dml row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  handle3.reset();
  merger.reset();
}

// insert1 -> delete2 -> insert2 -> delete3 ==> delete3
TEST_F(TestDeleteInsertMerge, test_insert_delete_one_trans)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -99        9       1     DELETE    NORMAL        UCF              trans_id_1\n"
      "1        var1  MIN      -80        9       1     INSERT    NORMAL UC               trans_id_1\n"
      "1        var1  MIN      -79        1       1     DELETE    NORMAL        UC               trans_id_1\n"
      "1        var1  MIN      -10        1       1     INSERT    NORMAL        UCL              trans_id_1\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 30;
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
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "2        var2  MIN      -99          9       9     DELETE    NORMAL        UCF               trans_id_3\n"
      "2        var2  MIN      -80          9       9     INSERT    NORMAL UC                trans_id_3\n"
      "2        var2  MIN      -79          2       2     DELETE    NORMAL        UC                trans_id_3\n"
      "2        var2  MIN      -60          2       2     INSERT    NORMAL        UCL               trans_id_3\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   var   bigint  bigint    bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -120       29     29     INSERT    NORMAL UCF         trans_id_4\n"
      "1        var1  MIN      -119       19     19     DELETE    NORMAL        UC          trans_id_4\n"
      "1        var1  MIN      -100       19     19     INSERT    NORMAL        UCL         trans_id_4\n";

  snapshot_version = 80;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(80);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
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

  for (int64_t i = 1; i <= 5; i++) {
    ObTxData *tx_data = new ObTxData();
    ASSERT_EQ(OB_SUCCESS, tx_data->init_tx_op());
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
  trans_version_range.multi_version_start_ = 3;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -40      MIN          29      29    INSERT    NORMAL SCF\n"
      "1        var1  -40      DI_VERSION    29      29    INSERT    NORMAL C\n"
      "1        var1  -10      0            9       1     DELETE    NORMAL        CL\n"
      "2        var2  -30      0            9       9     DELETE    NORMAL        CLF\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/, false/*cmp dml row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  handle3.reset();
  merger.reset();
}

TEST_F(TestDeleteInsertMerge, test_multi_dml_in_one_trans)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -100       9       9     INSERT    NORMAL        UCF              trans_id_2\n"
      "1        var1  MIN      -90        9       1     DELETE    NORMAL        UC               trans_id_2\n"
      "1        var1  MIN      -80        9       1     INSERT    NORMAL        UC               trans_id_2\n"
      "1        var1  MIN      -70        1       1     DELETE    NORMAL        UC               trans_id_2\n"
      "1        var1  -10      0          1       1     INSERT    NORMAL        CL               trans_id_0\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 30;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

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
    ASSERT_EQ(OB_SUCCESS, tx_data->init_tx_op());
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
  trans_version_range.multi_version_start_ = 3;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(BACKFILL_TX_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      MIN          9       9     INSERT    NORMAL SCF\n"
      "1        var1  -20      DI_VERSION    9       9     INSERT    NORMAL C\n"
      "1        var1  -20      0            1       1     DELETE    NORMAL        C\n"
      "1        var1  -10      DI_VERSION   1       1     INSERT    NORMAL        CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/, false/*cmp dml row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  merger.reset();
}

TEST_F(TestDeleteInsertMerge, test_accross_multi_macro)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -100       99     99     INSERT    NORMAL        UCF              trans_id_2\n"
      "1        var1  MIN      -90        99     1      DELETE    NORMAL        UC               trans_id_2\n"
      "1        var1  MIN      -80        99     1      INSERT    NORMAL        UC               trans_id_2\n"
      "1        var1  MIN      -70        59     59     DELETE    NORMAL        UC               trans_id_2\n";

  micro_data[1] =
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -60       59     59     INSERT    NORMAL        UC               trans_id_2\n"
      "1        var1  MIN      -50       59     1      DELETE    NORMAL        UC               trans_id_2\n"
      "1        var1  MIN      -40       59     1      INSERT    NORMAL        UC               trans_id_2\n"
      "1        var1  MIN      -30       19     19     DELETE    NORMAL        UC               trans_id_2\n";

  micro_data[2] =
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -20        99     19     INSERT    NORMAL        UC               trans_id_2\n"
      "1        var1  MIN      -10        1      1      DELETE    NORMAL        UC               trans_id_2\n"
      "1        var1  -10      0          1      1      INSERT    NORMAL        CL               trans_id_0\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 30;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(micro_data + 1, 1);
  prepare_one_macro(micro_data + 2, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

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
    ASSERT_EQ(OB_SUCCESS, tx_data->init_tx_op());
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
  trans_version_range.multi_version_start_ = 3;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(BACKFILL_TX_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      MIN          99      99    INSERT    NORMAL SCF\n"
      "1        var1  -20      DI_VERSION    99      99    INSERT    NORMAL C\n"
      "1        var1  -20      0            1       1     DELETE    NORMAL        C\n"
      "1        var1  -10      DI_VERSION   1       1     INSERT    NORMAL        CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/, false/*cmp dml row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  merger.reset();
}

TEST_F(TestDeleteInsertMerge, test_accross_multi_sstable)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -40        9       9     INSERT    NORMAL        UCF              trans_id_2\n"
      "1        var1  MIN      -30        9       1     DELETE    NORMAL        UC               trans_id_2\n"
      "1        var1  MIN      -20        9       1     INSERT    NORMAL        UC               trans_id_2\n"
      "1        var1  MIN      -10        1       1     DELETE    NORMAL        UC               trans_id_2\n"
      "1        var1  -10      0          1       1     INSERT    NORMAL        CL               trans_id_0\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 30;
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
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -100        19     19     INSERT    NORMAL        UCF              trans_id_2\n"
      "1        var1  MIN      -90         9       9     DELETE    NORMAL        UCL              trans_id_2\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
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

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  ls_handle.get_ls()->get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  for (int64_t i = 1; i <= 5; i++) {
    ObTxData *tx_data = new ObTxData();
    ASSERT_EQ(OB_SUCCESS, tx_data->init_tx_op());
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
  trans_version_range.multi_version_start_ = 3;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      MIN          19     19     INSERT    NORMAL SCF\n"
      "1        var1  -20      DI_VERSION    19     19     INSERT    NORMAL C\n"
      "1        var1  -20      0            1       1     DELETE    NORMAL        C\n"
      "1        var1  -10      DI_VERSION   1       1     INSERT    NORMAL        CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/, false/*cmp dml row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestDeleteInsertMerge, test_accross_multi_sstable2)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -30        9       1     DELETE    NORMAL        UCF              trans_id_2\n"
      "1        var1  MIN      -20        9       1     INSERT    NORMAL        UC               trans_id_2\n"
      "1        var1  MIN      -10        1       1     DELETE    NORMAL        UC               trans_id_2\n"
      "1        var1  -10      0          1       1     INSERT    NORMAL        CL               trans_id_0\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 30;
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
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -100        19     19     INSERT    NORMAL        UCF              trans_id_2\n"
      "1        var1  MIN      -90         9       9     DELETE    NORMAL        UC               trans_id_2\n"
      "1        var1  MIN      -40         9       9     INSERT    NORMAL        UCL              trans_id_2\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
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

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  ls_handle.get_ls()->get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  for (int64_t i = 1; i <= 5; i++) {
    ObTxData *tx_data = new ObTxData();
    ASSERT_EQ(OB_SUCCESS, tx_data->init_tx_op());
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
  trans_version_range.multi_version_start_ = 3;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      MIN          19     19     INSERT    NORMAL SCF\n"
      "1        var1  -20      DI_VERSION    19     19     INSERT    NORMAL C\n"
      "1        var1  -20      0            1       1     DELETE    NORMAL        C\n"
      "1        var1  -10      DI_VERSION   1       1     INSERT    NORMAL        CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/, false/*cmp dml row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestDeleteInsertMerge, test_delete_accross_multi_sstable)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -30        9       1     DELETE    NORMAL        UCF              trans_id_2\n"
      "1        var1  MIN      -20        9       1     INSERT    NORMAL        UC               trans_id_2\n"
      "1        var1  MIN      -10        1       1     DELETE    NORMAL        UC               trans_id_2\n"
      "1        var1  -10      0          1       1     INSERT    NORMAL        CL               trans_id_0\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 30;
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
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -90         9       9     DELETE    NORMAL        UCF              trans_id_2\n"
      "1        var1  MIN      -40         9       9     INSERT    NORMAL        UCL              trans_id_2\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
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

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  ls_handle.get_ls()->get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  for (int64_t i = 1; i <= 5; i++) {
    ObTxData *tx_data = new ObTxData();
    ASSERT_EQ(OB_SUCCESS, tx_data->init_tx_op());
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
  trans_version_range.multi_version_start_ = 3;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      MIN          1       1     DELETE    INSERT_DELETE SCF\n"
      "1        var1  -20      0            1       1     DELETE    NORMAL        C\n"
      "1        var1  -10      DI_VERSION   1       1     INSERT    NORMAL        CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/, false/*cmp dml row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestDeleteInsertMerge, test_insert_accross_multi_sstable_with_last_check)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -30        9       1     DELETE    NORMAL        UCF              trans_id_2\n"
      "1        var1  MIN      -20        9       1     INSERT    NORMAL        UC               trans_id_2\n"
      "1        var1  MIN      -10        1       1     DELETE    NORMAL        UC               trans_id_2\n"
      "1        var1  MIN      -1         1       1     INSERT    NORMAL        UCL              trans_id_2\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 30;
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
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -100        19      19    INSERT    NORMAL        UCF              trans_id_2\n"
      "1        var1  MIN      -90         9       9     DELETE    NORMAL        UC               trans_id_2\n"
      "1        var1  MIN      -40         9       9     INSERT    NORMAL        UCL              trans_id_2\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
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

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  ls_handle.get_ls()->get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  for (int64_t i = 1; i <= 5; i++) {
    ObTxData *tx_data = new ObTxData();
    ASSERT_EQ(OB_SUCCESS, tx_data->init_tx_op());
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
  trans_version_range.multi_version_start_ = 3;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      DI_VERSION  19      19     INSERT    NORMAL CF\n"
      "1        var1  -20      0          9       1      DELETE    NORMAL        CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/, false/*cmp dml row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestDeleteInsertMerge, test_insert_accross_multi_sstable_with_last_check2)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "0        var0  -10      -1         0       0     INSERT    NORMAL        CLF               trans_id_0\n"
      "1        var1  MIN      -1         1       1     INSERT    NORMAL        UCFL              trans_id_2\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 30;
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
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -500        9       9     DELETE    NORMAL        UCF              trans_id_4\n"
      "1        var1  MIN      -400        9       9     INSERT    NORMAL        UC               trans_id_2\n"
      "1        var1  MIN      -300        1       1     DELETE    NORMAL        UCL              trans_id_2\n"
      "2        var2  -50      0           1       1     INSERT    NORMAL        CLF              trans_id_0\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1, INT64_MAX, true);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

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
      transaction::ObUndoAction undo_action(ObTxSEQ(9, 0),ObTxSEQ(1, 0));
      tx_data->add_undo_action(tx_table, undo_action);
    }

    ASSERT_EQ(OB_SUCCESS, tx_table->insert(tx_data));
    delete tx_data;
  }

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 3;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "0        var0  -10      DI_VERSION  0       0     INSERT    NORMAL        CLF                trans_id_0\n"
      "1        var1  MIN      -500        9       9     DELETE    NORMAL        UCF                trans_id_4\n"
      "1        var1  -22      DI_VERSION  9       9     INSERT    NORMAL        CL                 trans_id_0\n"
      "2        var2  -50      DI_VERSION  1       1     INSERT    NORMAL        CLF                trans_id_0\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true, false);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestDeleteInsertMerge, test_multi_delete_accross_multi_sstable_with_last_check)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -30        9       1     DELETE    NORMAL        UCF              trans_id_2\n"
      "1        var1  MIN      -20        9       1     INSERT    NORMAL        UC               trans_id_2\n"
      "1        var1  MIN      -10        1       1     DELETE    NORMAL        UCL              trans_id_2\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 30;
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
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -90         9       9     DELETE    NORMAL        UCF              trans_id_2\n"
      "1        var1  MIN      -40         9       9     INSERT    NORMAL        UCL              trans_id_2\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
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

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  ls_handle.get_ls()->get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  for (int64_t i = 1; i <= 5; i++) {
    ObTxData *tx_data = new ObTxData();
    ASSERT_EQ(OB_SUCCESS, tx_data->init_tx_op());
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
  trans_version_range.multi_version_start_ = 3;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      0           1       1     DELETE    NORMAL        CLF\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/, false/*cmp dml row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestDeleteInsertMerge, test_multi_delete_accross_multi_sstable_with_shadow)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -30        9       1     DELETE    NORMAL        UCF              trans_id_2\n"
      "1        var1  MIN      -20        9       1     INSERT    NORMAL        UC               trans_id_2\n"
      "1        var1  MIN      -10        1       1     DELETE    NORMAL        UC               trans_id_2\n"
      "1        var1  -10        0        1       1     INSERT    NORMAL        CL               trans_id_0\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 30;
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
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -90         9       9     DELETE    NORMAL        UCF              trans_id_2\n"
      "1        var1  MIN      -40         9       9     INSERT    NORMAL        UCL              trans_id_2\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
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

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  ls_handle.get_ls()->get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  for (int64_t i = 1; i <= 5; i++) {
    ObTxData *tx_data = new ObTxData();
    ASSERT_EQ(OB_SUCCESS, tx_data->init_tx_op());
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
  trans_version_range.multi_version_start_ = 3;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      MIN         1       1     DELETE    INSERT_DELETE SCF\n"
      "1        var1  -20      0           1       1     DELETE    NORMAL        C\n"
      "1        var1  -10      DI_VERSION  1       1     INSERT    NORMAL        CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/, false/*cmp dml row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestDeleteInsertMerge, test_shadow_delete)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -10      MIN        1       1     DELETE    NORMAL        SCF\n"
      "1        var1  -10      0          1       1     DELETE    NORMAL        C\n"
      "1        var1  -8       DI_VERSION 1       1     INSERT    NORMAL        CL\n"
      "2        var2  -20      DI_VERSION 2       2     INSERT    NORMAL        CLF\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 30;
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
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -10      DI_VERSION    19     19     INSERT    NORMAL  CLF\n";

  snapshot_version = 50;
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
  trans_version_range.multi_version_start_ = 3;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -10      MIN          19     19     INSERT    NORMAL        SCF\n"
      "1        var1  -10      DI_VERSION   19     19     INSERT    NORMAL        C\n"
      "1        var1  -10      0            1       1     DELETE    NORMAL        C\n"
      "1        var1  -8       DI_VERSION   1       1     INSERT    NORMAL        CL\n"
      "2        var2  -20      DI_VERSION   2       2     INSERT    NORMAL        CLF\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/, false/*cmp dml row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestDeleteInsertMerge, test_recycle_by_ha_status)
{
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  merge_context.static_param_.is_ha_compeleted_ = false;
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -30      MIN         1       1     INSERT    NORMAL        SCF\n"
      "1        var1  -30      DI_VERSION  1       1     INSERT    NORMAL        C\n"
      "1        var1  -30      0           1       1     DELETE    NORMAL        C\n"
      "1        var1  -20      DI_VERSION  1       1     INSERT    NORMAL        C\n"
      "1        var1  -20      0           1       1     DELETE    NORMAL        C\n"
      "1        var1  -10      DI_VERSION  1       1     INSERT    NORMAL        C\n"
      "1        var1  -10      0           1       1     DELETE    NORMAL        CL\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 30;
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
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -40      0           1     1     DELETE    NORMAL  CLF\n";

  snapshot_version = 50;
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
  trans_version_range.multi_version_start_ = 30;
  trans_version_range.base_version_ = 10;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor merge
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -40      MIN          1       1     DELETE    NORMAL        SCF\n"
      "1        var1  -40      0          1       1     DELETE    NORMAL        C\n"
      "1        var1  -30      DI_VERSION  1       1     INSERT    NORMAL        C\n"
      "1        var1  -30      0          1       1     DELETE    NORMAL        C\n"
      "1        var1  -20      DI_VERSION  1       1     INSERT    NORMAL        C\n"
      "1        var1  -20      0          1       1     DELETE    NORMAL        C\n"
      "1        var1  -10      DI_VERSION  1       1     INSERT    NORMAL        C\n"
      "1        var1  -10      0           1       1     DELETE    NORMAL        CL\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/, false/*cmp dml row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_delete_insert_minor_merge.log*");
  OB_LOGGER.set_file_name("test_delete_insert_minor_merge.log");
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
