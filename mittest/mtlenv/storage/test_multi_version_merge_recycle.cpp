// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.


#include <gtest/gtest.h>
#define private public
#define protected public
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
namespace storage
{

int clear_tx_data(ObTxDataTable *tx_data_table)
{
  int ret = OB_SUCCESS;
  ObTxDataMemtableMgr *mgr = tx_data_table->memtable_mgr_;
  ObTxDataMemtableWriteGuard write_guard;
  ObTxDataMemtable *tx_data_memtable = nullptr;
  if (OB_FAIL(mgr->get_all_memtables_for_write(write_guard))) {
    STORAGE_LOG(WARN, "get all memtables for write fail.", KR(ret), KPC(mgr));
  } else {
    ObTransID tx_id;
    ObTableHandleV2 (&memtable_handles)[MAX_TX_DATA_MEMTABLE_CNT] = write_guard.handles_;
    for (int i = write_guard.size_ - 1; OB_SUCC(ret) && i >= 0; i--) {
      tx_data_memtable = nullptr;
      if (OB_FAIL(memtable_handles[i].get_tx_data_memtable(tx_data_memtable))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "get tx data memtable from table handles fail.", KR(ret),
                    K(memtable_handles[i]));
      } else if (OB_ISNULL(tx_data_memtable)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "tx data memtable is nullptr.", KR(ret), K(memtable_handles[i]));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < 10; i++) {
          tx_id = i;
          if (tx_data_memtable->contain_tx_data(tx_id)) {
            ObTxData *existed_tx_data = nullptr;
            if (OB_FAIL(tx_data_memtable->get_tx_data(tx_id.tx_id_, existed_tx_data))) {
              STORAGE_LOG(WARN, "get tx data from tx data memtable failed.", KR(ret), K(i), KPC(tx_data_memtable));
            } else if (OB_ISNULL(existed_tx_data)) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(WARN, "existed tx data is unexpected nullptr", KR(ret),
                          KPC(tx_data_memtable));
            } else if (OB_FAIL(tx_data_memtable->remove(tx_id.tx_id_))) {
              STORAGE_LOG(ERROR, "remove tx data from tx data memtable failed.", KR(ret), K(tx_id), KPC(tx_data_memtable));
            }
          }
        }
      }
    }
  }
  return ret;
};

class TestMultiVersionMergeRecycle : public ObMultiVersionSSTableTest
{
public:
  static const int64_t MAX_PARALLEL_DEGREE = 10;
  TestMultiVersionMergeRecycle();
  virtual ~TestMultiVersionMergeRecycle() {}

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

public:
  ObStorageSchema table_merge_schema_;
  ObStoreCtx store_ctx_;
};

void TestMultiVersionMergeRecycle::SetUpTestCase()
{
  ObMultiVersionSSTableTest::SetUpTestCase();
  // mock sequence no
  ObClockGenerator::init();

  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  // create tablet
  ObLSTabletService *ls_tablet_svr = ls_handle.get_ls()->get_tablet_svr();
  uint64_t table_id = 12345;
  share::schema::ObTableSchema table_schema;
  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, table_id));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema));
}

void TestMultiVersionMergeRecycle::TearDownTestCase()
{
  ObMultiVersionSSTableTest::TearDownTestCase();
  // reset sequence no
  ObClockGenerator::destroy();
}

TestMultiVersionMergeRecycle::TestMultiVersionMergeRecycle()
  : ObMultiVersionSSTableTest("test_multi_version_merge_recycle")
{}

void TestMultiVersionMergeRecycle::SetUp()
{
  ObMultiVersionSSTableTest::SetUp();
}

void TestMultiVersionMergeRecycle::TearDown()
{
  ObMultiVersionSSTableTest::TearDown();
  TRANS_LOG(INFO, "teardown success");
}

void TestMultiVersionMergeRecycle::prepare_query_param(const ObVersionRange &version_range)
{
  context_.reset();
  ObLSID ls_id(ls_id_);
  iter_param_.table_id_ = table_id_;
  iter_param_.tablet_id_ = tablet_id_;
  iter_param_.read_info_ = &full_read_info_;
  iter_param_.full_read_info_ = &full_read_info_;
  iter_param_.out_cols_project_ = nullptr;
  iter_param_.is_same_schema_column_ = true;
  iter_param_.has_virtual_columns_ = false;
  iter_param_.vectorized_enabled_ = false;
  ASSERT_EQ(OB_SUCCESS,
            store_ctx_.init_for_read(ls_id,
                                     INT64_MAX, // query_expire_ts
                                     -1, // lock_timeout_us
                                     INT64_MAX - 2));
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

void TestMultiVersionMergeRecycle::prepare_merge_context(const ObMergeType &merge_type,
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
  merge_context.schema_ctx_.merge_schema_ = &table_merge_schema_;

  merge_context.is_full_merge_ = is_full_merge;
  merge_context.merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
  merge_context.param_.merge_type_ = merge_type;
  merge_context.param_.merge_version_ = 0;
  merge_context.param_.ls_id_ = ls_id_;
  merge_context.param_.tablet_id_ = tablet_id_;
  merge_context.sstable_version_range_ = trans_version_range;
  merge_context.param_.report_ = &rs_reporter_;
  merge_context.progressive_merge_num_ = 0;
  const common::ObIArray<ObITable *> &tables = merge_context.tables_handle_.get_tables();
  merge_context.log_ts_range_.start_log_ts_ = tables.at(0)->get_start_log_ts();
  merge_context.log_ts_range_.end_log_ts_ = tables.at(tables.count() - 1)->get_end_log_ts();

  ASSERT_EQ(OB_SUCCESS, merge_context.init_merge_info());
  ASSERT_EQ(OB_SUCCESS, merge_context.merge_info_.prepare_index_builder(index_desc_));
}

void TestMultiVersionMergeRecycle::build_sstable(
    ObTabletMergeCtx &ctx,
    ObSSTable *&merged_sstable)
{
  ASSERT_EQ(OB_SUCCESS, ctx.merge_info_.create_sstable(ctx));
}

TEST_F(TestMultiVersionMergeRecycle, recycle_macro)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var1  -6       0        NOP      1    EXIST   LF\n"
      "1        var1  -6       0        2        2    EXIST   CLF\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "2        var1  -10       0        3       NOP   EXIST   LF\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 20;
  ObLogTsRange log_ts_range;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 10;
  prepare_table_schema(micro_data, schema_rowkey_cnt, log_ts_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[2];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "3        var3  -8       MIN        2     12    EXIST   SCF\n"
      "3        var3  -8       0          NOP   12    EXIST   N\n"
      "3        var3  -6       0          2     2    EXIST   CL\n";

  micro_data2[1] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "5        var5  -10       0        NOP     13    EXIST   LF\n";

  snapshot_version = 30;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 20;
  table_key_.log_ts_range_ = log_ts_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_one_macro(&micro_data2[1], 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 10;
  trans_version_range.base_version_ = 9;

  prepare_merge_context(MINI_MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "2        var1  -10       0        3       NOP   EXIST   LF\n"
      "5        var5  -10       0        NOP     13    EXIST   LF\n";

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

TEST_F(TestMultiVersionMergeRecycle, recycle_after_reuse)
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
      "1        var1  -15       0        12       NOP    EXIST  N\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "1        var1  -7        0        2       2    EXIST   CL\n"
      "2        var2  -8        0        3       NOP   EXIST   LF\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 20;
  ObLogTsRange log_ts_range;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 10;
  prepare_table_schema(micro_data, schema_rowkey_cnt, log_ts_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[2];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "3        var3  -8       MIN        12     12    EXIST   SCF\n"
      "3        var3  -8       0          NOP   12    EXIST   N\n"
      "3        var3  -6       0          12    NOP    EXIST  N\n";

  micro_data2[1] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "3        var3  -4       0          2    2      EXIST  CL\n"
      "5        var5  -10      0        NOP    13    EXIST   LF\n";

  snapshot_version = 30;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 20;
  table_key_.log_ts_range_ = log_ts_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_one_macro(&micro_data2[1], 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 10;
  trans_version_range.base_version_ = 9;

  prepare_merge_context(MINI_MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "0        var0  -15       0        NOP      1    EXIST   LF\n"
      "1        var1  -15       MIN      12       2    EXIST  SCF\n"
      "1        var1  -15       0        12       NOP    EXIST  N\n"
      "1        var1  -7        0        2       2    EXIST   CL\n"
      "2        var2  -8        0        3       NOP   EXIST   LF\n"
      "5        var5  -10       0        NOP     13    EXIST   LF\n";

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

TEST_F(TestMultiVersionMergeRecycle, reuse_after_recycle)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -8       0        NOP      1    EXIST   LF\n"
      "1        var1  -8       MIN      12       2    EXIST  SCF\n"
      "1        var1  -8       0        12       NOP    EXIST  N\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "1        var1  -7        0        2       2    EXIST   CL\n"
      "2        var2  -15        0        3       NOP   EXIST   LF\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 20;
  ObLogTsRange log_ts_range;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 10;
  prepare_table_schema(micro_data, schema_rowkey_cnt, log_ts_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[2];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "3        var3  -15       MIN        12     12    EXIST   SCF\n"
      "3        var3  -15       0          NOP   12    EXIST   N\n"
      "3        var3  -8       0          12    NOP    EXIST  L\n";

  micro_data2[1] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "4        var4  -4       0          2    2      EXIST  CLF\n"
      "5        var5  -6      0        NOP    13    EXIST   LF\n";

  snapshot_version = 30;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 20;
  table_key_.log_ts_range_ = log_ts_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_one_macro(&micro_data2[1], 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 10;
  trans_version_range.base_version_ = 9;

  prepare_merge_context(MINI_MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "2        var2  -15        0        3       NOP   EXIST   LF\n"
      "3        var3  -15       MIN        12     12    EXIST   SCF\n"
      "3        var3  -15       0          NOP   12    EXIST   N\n"
      "3        var3  -8       0          12    NOP    EXIST  L\n";

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

TEST_F(TestMultiVersionMergeRecycle, recycled_micros_after_reuse)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -15       0        NOP      1    EXIST   LF\n"
      "1        var1  -15       MIN      12       2    EXIST  SCF\n"
      "1        var1  -15       0        12       NOP    EXIST  N\n"
      "1        var1  -10       0        2         2    EXIST  CL\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "2        var2  -8        0        3       NOP   EXIST   LF\n"
      "3        var3  -8        MIN      12      12   EXIST   SCF\n"
      "3        var3  -8        0       12      NOP   EXIST   N\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "3        var3  -6        0      NOP      12   EXIST   L\n"
      "4        var4  -15       0      2        2    EXIST   CLF\n"
      "5        var5  -15       0      NOP      12   EXIST   LF\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 20;
  ObLogTsRange log_ts_range;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 10;
  prepare_table_schema(micro_data, schema_rowkey_cnt, log_ts_range, snapshot_version);
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
      "5        var5  -20      0        NOP    13    EXIST   LF\n";

  snapshot_version = 30;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 20;
  table_key_.log_ts_range_ = log_ts_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 10;
  trans_version_range.base_version_ = 9;

  prepare_merge_context(MINI_MINOR_MERGE, false, trans_version_range, merge_context);
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

TEST_F(TestMultiVersionMergeRecycle, rowkeys_across_micros)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[5];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -10       0        NOP      1    EXIST   LF\n"
      "1        var1  -10       MIN      12       2    EXIST  SCF\n"
      "1        var1  -10       0        12       NOP    EXIST  N\n"
      "1        var1  -8        0        2         2    EXIST  C\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "1        var1  -7        0        1         2    EXIST  C\n"
      "1        var1  -6        0        2         1    EXIST  C\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "1        var1  -5        0        1         1    EXIST  CL\n"
      "2        var2  -15       MIN      12      12   EXIST   SCF\n"
      "2        var2  -15       0      NOP     12    EXIST   N\n";

  micro_data[3] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "2        var2  -10       0      12      NOP   EXIST   N\n"
      "2        var2  -8        0      2       2     EXIST   N\n";

  micro_data[4] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "2        var2  -6        0      1       1     EXIST   CL\n"
      "3        var3  -15       0      2       2     EXIST   CLF\n"
      "5        var5  -15      0      2        2    EXIST   CLF\n";


  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 20;
  ObLogTsRange log_ts_range;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 20;
  prepare_table_schema(micro_data, schema_rowkey_cnt, log_ts_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 5);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "5        var5  -20      0        NOP    13    EXIST   LF\n";

  snapshot_version = 30;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 30;
  table_key_.log_ts_range_ = log_ts_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 14;
  trans_version_range.base_version_ = 12;

  prepare_merge_context(MINI_MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "2        var2  -15       MIN      12      12   EXIST   SCF\n"
      "2        var2  -15       0      NOP     12    EXIST   N\n"
      "2        var2  -10       0      12      2   EXIST     CL\n"
      "3        var3  -15       0      2       2   EXIST   CLF\n"
      "5        var5  -20       MIN     2     13   EXIST   SCF\n"
      "5        var5  -20       0      NOP     13   EXIST   N\n"
      "5        var5  -15       0      2     2   EXIST   CL\n";

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

TEST_F(TestMultiVersionMergeRecycle, rowkeys_across_macro)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[5];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -10       0        NOP      1    EXIST   LF\n"
      "1        var1  -15       MIN      12       2    EXIST  SCF\n"
      "1        var1  -15       0        12       NOP    EXIST  N\n"
      "1        var1  -8        0        2         2    EXIST  C\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "1        var1  -7        0        1         2    EXIST  C\n"
      "1        var1  -6        0        2         1    EXIST  C\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "1        var1  -5        0        1         1    EXIST  CL\n"
      "2        var2  -15       MIN      12      12   EXIST   SCF\n"
      "2        var2  -15       0      NOP     12    EXIST   N\n";

  micro_data[3] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "2        var2  -10       0      12      NOP   EXIST   N\n"
      "2        var2  -8        0      2       2     EXIST   N\n";

  micro_data[4] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "2        var2  -6        0      1       1     EXIST   CL\n"
      "3        var3  -15       0      2       2     EXIST   CLF\n"
      "5        var5  -15      0      2        2    EXIST   CLF\n";


  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 20;
  ObLogTsRange log_ts_range;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 20;
  prepare_table_schema(micro_data, schema_rowkey_cnt, log_ts_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_one_macro(&micro_data[2], 1);
  prepare_one_macro(&micro_data[3], 1);
  prepare_one_macro(&micro_data[4], 1);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "5        var5  -20      0        NOP    13    EXIST   LF\n";

  snapshot_version = 30;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 30;
  table_key_.log_ts_range_ = log_ts_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 14;
  trans_version_range.base_version_ = 12;

  prepare_merge_context(MINI_MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "0        var0  -10       0        NOP      1    EXIST   LF\n"
      "1        var1  -15       MIN      12       2    EXIST  SCF\n"
      "1        var1  -15       0        12       NOP    EXIST  N\n"
      "1        var1  -8        0        2         2    EXIST  C\n"
      "1        var1  -7        0        1         2    EXIST  C\n"
      "1        var1  -5        0        1         1    EXIST  CL\n"
      "2        var2  -15       MIN      12      12   EXIST   SCF\n"
      "2        var2  -15       0      NOP     12    EXIST   N\n"
      "2        var2  -10       0      12      2   EXIST     C\n"
      "2        var2  -6       0       1      1   EXIST     CL\n"
      "3        var3  -15       0      2       2   EXIST   CLF\n"
      "5        var5  -20       MIN     2     13   EXIST   SCF\n"
      "5        var5  -20       0      NOP     13   EXIST   N\n"
      "5        var5  -15       0      2     2   EXIST   CL\n";

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

TEST_F(TestMultiVersionMergeRecycle, recycle_macro_with_last_row)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -10       0        NOP      1    EXIST   LF\n"
      "1        var1  -10       MIN      12       2    EXIST  SCF\n"
      "1        var1  -10       0        12       NOP    EXIST  N\n"
      "1        var1  -8        0        2         2    EXIST  CL\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "2        var2  -10       MIN      12      12   EXIST   SCF\n"
      "2        var2  -10       0      NOP     12    EXIST   N\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "2        var2  -9        0      12     NOP     EXIST   N\n"
      "2        var2  -8        0      2       2     EXIST   CL\n"
      "3        var3  -15       0      2       2     EXIST   CLF\n"
      "5        var5  -15      0      2        2    EXIST   CLF\n";


  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 20;
  ObLogTsRange log_ts_range;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 20;
  prepare_table_schema(micro_data, schema_rowkey_cnt, log_ts_range, snapshot_version);
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
      "5        var5  -20      0        NOP    13    EXIST   LF\n";

  snapshot_version = 30;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 30;
  table_key_.log_ts_range_ = log_ts_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 14;
  trans_version_range.base_version_ = 12;

  prepare_merge_context(MINI_MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "3        var3  -15       0      2       2   EXIST   CLF\n"
      "5        var5  -20       MIN     2     13   EXIST   SCF\n"
      "5        var5  -20       0      NOP     13   EXIST   N\n"
      "5        var5  -15       0      2     2   EXIST   CL\n";

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

TEST_F(TestMultiVersionMergeRecycle, reuse_after_recycle_with_last)
{
  int ret = OB_SUCCESS;
  ObPartitionMinorMerger merger;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[4];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -10       0        NOP      1    EXIST   LF\n"
      "1        var1  -10       MIN      12       2    EXIST  SCF\n"
      "1        var1  -10       0        12       NOP    EXIST  N\n"
      "1        var1  -8        0        2         2    EXIST  CL\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "2        var2  -15       0        2     2    EXIST   CLF\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "3        var3  -15       0      2       2     EXIST   CLF\n";

  micro_data[3] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "5        var5  -15      0      2        2    EXIST   CLF\n";


  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 20;
  ObLogTsRange log_ts_range;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 20;
  prepare_table_schema(micro_data, schema_rowkey_cnt, log_ts_range, snapshot_version);
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
      "5        var5  -20      0        NOP    13    EXIST   LF\n";

  snapshot_version = 30;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 30;
  table_key_.log_ts_range_ = log_ts_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 14;
  trans_version_range.base_version_ = 12;

  prepare_merge_context(MINI_MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "2        var2  -15       0        2     2    EXIST   CLF\n"
      "3        var3  -15       0      2       2   EXIST   CLF\n"
      "5        var5  -20       MIN     2     13   EXIST   SCF\n"
      "5        var5  -20       0      NOP     13   EXIST   N\n"
      "5        var5  -15       0      2     2   EXIST   CL\n";

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
  system("rm -rf test_multi_version_merge_recycle.log*");
  OB_LOGGER.set_file_name("test_multi_version_merge_recycle.log");
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
