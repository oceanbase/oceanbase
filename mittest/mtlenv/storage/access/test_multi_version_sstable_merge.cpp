// owner: dengzhi.ldz
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
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/compaction/ob_partition_merger.h"
#include "storage/test_tablet_helper.h"
#include "test_merge_basic.h"
#include "storage/mockcontainer/mock_ob_merge_iterator.h"

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

int clear_tx_data()
{
  TX_DATA_ARR.reset();
  return OB_SUCCESS;
};


class TestMultiVersionMerge : public TestMergeBasic
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
  void get_tx_table_guard(ObTxTableGuard &tx_table_guard);
  void prepare_output_expr(const ObIArray<int32_t> &projector,
                           const ObIArray<ObColDesc> &cols_desc);
  void prepare_scan_param(const ObVersionRange &version_range,
                          const ObTableStoreIterator &table_store_iter);
public:
  static const int64_t DATUM_ARRAY_CNT = 1024;
  static const int64_t DATUM_RES_SIZE = 10;
  static const int64_t SQL_BATCH_SIZE = 256;
  ObArenaAllocator query_allocator_;
  ObStoreCtx store_ctx_;
  ObTabletMergeExecuteDag merge_dag_;
  ObTableAccessParam access_param_;
  ObGetTableParam get_table_param_;
  ObTableReadInfo read_info_;
  ObFixedArray<int32_t, ObIAllocator> output_cols_project_;
  ObFixedArray<share::schema::ObColumnParam*, ObIAllocator> cols_param_;
  sql::ExprFixedArray output_exprs_;
  sql::ObExecContext exec_ctx_;
  sql::ObEvalCtx eval_ctx_;
  sql::ObPushdownExprSpec expr_spec_;
  sql::ObPushdownOperator op_;
  void *datum_buf_;
  int64_t datum_buf_offset_;
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
  MERGE_SCHEDULER_PTR->resume_major_merge();

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
  : TestMergeBasic("test_multi_version_merge"),
    exec_ctx_(query_allocator_),
    eval_ctx_(exec_ctx_),
    expr_spec_(query_allocator_),
    op_(eval_ctx_, expr_spec_)
{}

void TestMultiVersionMerge::SetUp()
{
  ObMultiVersionSSTableTest::SetUp();
}

void TestMultiVersionMerge::fake_freeze_info()
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

void TestMultiVersionMerge::prepare_output_expr(
    const ObIArray<int32_t> &projector,
    const ObIArray<ObColDesc> &cols_desc)
{
  output_exprs_.set_allocator(&query_allocator_);
  output_exprs_.init(projector.count());
  for (int64_t i = 0; i < projector.count(); ++i) {
    void *expr_buf = query_allocator_.alloc(sizeof(sql::ObExpr));
    ASSERT_NE(nullptr, expr_buf);
    sql::ObExpr *expr = reinterpret_cast<sql::ObExpr *>(expr_buf);
    expr->reset();

    expr->frame_idx_ = 0;
    expr->datum_off_ = datum_buf_offset_;
    sql::ObDatum *datums = new ((char*)datum_buf_ + datum_buf_offset_) sql::ObDatum[DATUM_ARRAY_CNT];
    datum_buf_offset_ += sizeof(sql::ObDatum) * DATUM_ARRAY_CNT;
    expr->res_buf_off_ = datum_buf_offset_;
    expr->res_buf_len_ = DATUM_RES_SIZE;
    char *ptr = (char *)datum_buf_ + expr->res_buf_off_;
    for (int64_t i = 0; i < DATUM_ARRAY_CNT; i++) {
      datums[i].ptr_ = ptr;
      ptr += expr->res_buf_len_;
    }
    datum_buf_offset_ += expr->res_buf_len_ * DATUM_ARRAY_CNT;
    expr->type_ = T_REF_COLUMN;
    expr->obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
    expr->batch_result_ = true;
    expr->datum_meta_.type_ = cols_desc.at(i).col_type_.get_type();
    expr->obj_meta_ = cols_desc.at(i).col_type_;
    output_exprs_.push_back(expr);
  }
}

void TestMultiVersionMerge::prepare_scan_param(
    const ObVersionRange &version_range,
    const ObTableStoreIterator &table_store_iter)
{
  context_.reset();
  access_param_.reset();
  get_table_param_.reset();
  read_info_.reset();
  output_cols_project_.reset();
  cols_param_.reset();
  output_exprs_.reset();
  datum_buf_ = nullptr;
  datum_buf_offset_ = 0;
  query_allocator_.reset();

  ObTabletHandle tablet_handle;
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));

  get_table_param_.frozen_version_ = INT64_MAX;
  get_table_param_.refreshed_merge_ = nullptr;
  get_table_param_.need_split_dst_table_ = false;
  get_table_param_.tablet_iter_.tablet_handle_ = tablet_handle;
  get_table_param_.tablet_iter_.table_store_iter_.assign(table_store_iter);
  get_table_param_.tablet_iter_.transfer_src_handle_ = nullptr;
  get_table_param_.tablet_iter_.split_extra_tablet_handles_.reset();

  int64_t schema_column_count = full_read_info_.get_schema_column_count();
  int64_t schema_rowkey_count = full_read_info_.get_schema_rowkey_count();
  eval_ctx_.batch_idx_ = 0;
  eval_ctx_.batch_size_ = SQL_BATCH_SIZE;
  expr_spec_.max_batch_size_ = SQL_BATCH_SIZE;
  datum_buf_ = query_allocator_.alloc((sizeof(sql::ObDatum) + OBJ_DATUM_NUMBER_RES_SIZE) * DATUM_ARRAY_CNT * 2 * schema_column_count);
  ASSERT_NE(nullptr, datum_buf_);
  eval_ctx_.frames_ = (char **)(&datum_buf_);

  output_cols_project_.set_allocator(&query_allocator_);
  output_cols_project_.init(schema_column_count);
  for (int64_t i = 0; i < schema_column_count; i++) {
    output_cols_project_.push_back(i);
  }

  ObSEArray<ObColDesc, 8> tmp_col_descs;
  ObSEArray<int32_t, 8> tmp_cg_idxs;
  const common::ObIArray<ObColDesc> &cols_desc = full_read_info_.get_columns_desc();
  for (int64_t i = 0; i < schema_column_count; i++) {
    if (i < schema_rowkey_count) {
      tmp_col_descs.push_back(cols_desc.at(i));
    } else {
      tmp_col_descs.push_back(cols_desc.at(i + 2));
    }
  }

  cols_param_.set_allocator(&query_allocator_);
  cols_param_.init(tmp_col_descs.count());
  for (int64_t i = 0; i < tmp_col_descs.count(); ++i) {
    void *col_param_buf = query_allocator_.alloc(sizeof(ObColumnParam));
    ObColumnParam *col_param = new(col_param_buf) ObColumnParam(query_allocator_);
    col_param->set_meta_type(tmp_col_descs.at(i).col_type_);
    col_param->set_nullable_for_write(true);
    col_param->set_column_id(common::OB_APP_MIN_COLUMN_ID + i);
    cols_param_.push_back(col_param);
    tmp_cg_idxs.push_back(i + 1);
  }

  ASSERT_EQ(OB_SUCCESS,
            read_info_.init(query_allocator_,
                            full_read_info_.get_schema_column_count(),
                            full_read_info_.get_schema_rowkey_count(),
                            lib::is_oracle_mode(),
                            tmp_col_descs,
                            nullptr/*storage_cols_index*/,
                            &cols_param_,
                            &tmp_cg_idxs));
  access_param_.iter_param_.read_info_ = &read_info_;
  access_param_.iter_param_.rowkey_read_info_ = &full_read_info_;

  access_param_.iter_param_.table_id_ = table_id_;
  access_param_.iter_param_.tablet_id_ = tablet_id_;
  access_param_.iter_param_.is_same_schema_column_ = true;
  access_param_.iter_param_.has_virtual_columns_ = false;
  access_param_.iter_param_.vectorized_enabled_ = true;
  access_param_.iter_param_.has_lob_column_out_ = false;
  access_param_.iter_param_.pd_storage_flag_.pd_blockscan_ = true;
  access_param_.iter_param_.pd_storage_flag_.pd_filter_ = true;
  access_param_.iter_param_.is_delete_insert_ = false;

  prepare_output_expr(output_cols_project_, tmp_col_descs);
  access_param_.iter_param_.out_cols_project_ = &output_cols_project_;
  access_param_.output_exprs_ = &output_exprs_;
  access_param_.iter_param_.output_exprs_ = &output_exprs_;
  access_param_.iter_param_.op_ = &op_;
  access_param_.padding_cols_ = nullptr;
  access_param_.aggregate_exprs_ = nullptr;
  access_param_.op_filters_ = nullptr;
  access_param_.output_sel_mask_ = nullptr;
  void *buf = query_allocator_.alloc(sizeof(ObRow2ExprsProjector));
  access_param_.row2exprs_projector_ = new (buf) ObRow2ExprsProjector(query_allocator_);
  // TODO: construct pushdown filter
  ASSERT_EQ(OB_SUCCESS, access_param_.iter_param_.op_->init_pushdown_storage_filter());
  access_param_.is_inited_ = true;

  ASSERT_EQ(OB_SUCCESS,
            store_ctx_.init_for_read(ls_id,
                                     tablet_id,
                                     INT64_MAX, // query_expire_ts
                                     -1, // lock_timeout_us
                                     share::SCN::max_scn()));

  ObQueryFlag query_flag(ObQueryFlag::NoOrder,
                         false, // daily_merge
                         false, // optimize
                         false, // sys scan
                         false, // full_row
                         false, // index_back
                         false, // query_stat
                         ObQueryFlag::MysqlMode, // sql_mode
                         true // read_latest
                        );
  query_flag.set_not_use_row_cache();
  query_flag.set_not_use_block_cache();
  //query_flag.multi_version_minor_merge_ = true;
  ASSERT_EQ(OB_SUCCESS,
            context_.init(query_flag,
                          store_ctx_,
                          query_allocator_,
                          query_allocator_,
                          version_range));
  context_.ls_id_ = ls_id_;
  context_.limit_param_ = nullptr;
  context_.is_inited_ = true;
}

void TestMultiVersionMerge::prepare_merge_context(const ObMergeType &merge_type,
                                                  const bool is_full_merge,
                                                  const ObVersionRange &trans_version_range,
                                                  ObTabletMergeCtx &merge_context)
{
  TestMergeBasic::prepare_merge_context(merge_type, is_full_merge, trans_version_range, merge_context);
  merge_context.static_param_.is_delete_insert_merge_ = false;
  merge_context.static_param_.data_version_ = DATA_VERSION_4_2_0_0;
  ASSERT_EQ(OB_SUCCESS, merge_context.cal_merge_param());
  ASSERT_EQ(OB_SUCCESS, merge_context.init_parallel_merge_ctx());
  ASSERT_EQ(OB_SUCCESS, merge_context.static_param_.init_static_info(merge_context.tablet_handle_));
  ASSERT_EQ(OB_SUCCESS, merge_context.init_static_desc());
  ASSERT_EQ(OB_SUCCESS, merge_context.init_read_info());
  ASSERT_EQ(OB_SUCCESS, merge_context.init_tablet_merge_info());
  ASSERT_EQ(OB_SUCCESS, merge_context.merge_info_.prepare_sstable_builder());
  ASSERT_EQ(OB_SUCCESS, merge_context.merge_info_.sstable_builder_.data_store_desc_.init(merge_context.static_desc_, table_merge_schema_));
  ASSERT_EQ(OB_SUCCESS, merge_context.merge_info_.prepare_index_builder());
  merge_context.merge_dag_ = &merge_dag_;
  merge_context.static_param_.for_unittest_ = true;
}

void TestMultiVersionMerge::build_sstable(
    ObTabletMergeCtx &ctx,
    ObSSTable *&merged_sstable)
{
  bool tmp_bool = false; // placeholder
  ASSERT_EQ(OB_SUCCESS, ctx.merge_info_.create_sstable(ctx, ctx.merged_table_handle_, tmp_bool));
  ASSERT_EQ(OB_SUCCESS, ctx.merged_table_handle_.get_sstable(merged_sstable));
}

void TestMultiVersionMerge::get_tx_table_guard(ObTxTableGuard &tx_table_guard)
{
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tx_table_guard(tx_table_guard));
}



TEST_F(TestMultiVersionMerge, rowkey_cross_two_macro_and_second_macro_is_filtered)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

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
  merge_context.static_param_.tables_handle_.add_table(handle1);
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
  merge_context.static_param_.tables_handle_.add_table(handle2);
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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, rowkey_cross_three_macro_inc_merge)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

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
  merge_context.static_param_.tables_handle_.add_table(handle1);
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
  merge_context.static_param_.tables_handle_.add_table(handle2);
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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, uncommit_rowkey_committed_in_minor)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint  dml  flag    multi_version_row_flag trans_id\n"
      "0        var1  -8       0        NOP      1     T_DML_UPDATE EXIST   LF  trans_id_0\n"
      "1        var1  MIN       0        10       NOP   T_DML_UPDATE  EXIST   FU  trans_id_1\n"
      "1        var1  -8       MIN      3         3    T_DML_UPDATE  EXIST   SC  trans_id_0\n"
      "1        var1  -8       0        3        NOP   T_DML_UPDATE  EXIST   N  trans_id_0\n";

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
  merge_context.static_param_.tables_handle_.add_table(handle1);
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
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "0        var1  -10      MIN      NOP      10     EXIST   SF\n"
      "0        var1  -10      0        NOP      10     EXIST   N\n"
      "0        var1  -8       0        NOP      1      EXIST   L\n"
      "1        var1  -10       MIN      10       3      EXIST   SCF\n"
      "1        var1  -10       0        10       NOP    EXIST   N\n"
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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, uncommit_rowkey_in_one_macro_committed_is_last)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

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
  merge_context.static_param_.tables_handle_.add_table(handle1);
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
  merge_context.static_param_.tables_handle_.add_table(handle2);
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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, uncommit_rowkey_in_one_macro_committed_following_last)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint   flag    multi_version_row_flag trans_id\n"
      "0        var1  -8       0        NOP      1      EXIST   LF  trans_id_0\n"
      "1        var1  MIN       0        10       NOP     EXIST  FU  trans_id_1\n";

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
  merge_context.static_param_.tables_handle_.add_table(handle1);
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
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "0        var1  -10      -9223372036854775807  NOP      10     EXIST   SF\n"
      "0        var1  -10      0        NOP      10     EXIST   N\n"
      "0        var1  -8       0        NOP      1      EXIST   L\n"
      "1        var1  -10      -9223372036854775807  10 3 EXIST  SCF\n"
      "1        var1  -10       0        10       NOP    EXIST   N\n"
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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, uncommit_rowkey_in_one_macro_committed_following_shadow)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint   flag    multi_version_row_flag trans_id\n"
      "0        var1  -8       0        NOP      1      EXIST   LF  trans_id_0\n"
      "1        var1  MIN       0        10       NOP     EXIST  FU  trans_id_1\n";

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
  merge_context.static_param_.tables_handle_.add_table(handle1);
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
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "0        var1  -10      -9223372036854775807  NOP      10     EXIST   SF\n"
      "0        var1  -10      0        NOP      10     EXIST   N\n"
      "0        var1  -8       0        NOP      1      EXIST   L\n"
      "1        var1  -10      -9223372036854775807  10 3 EXIST   SCF\n"
      "1        var1  -10       0        10       NOP    EXIST   N\n"
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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, rowkey_cross_three_macro_full_merge)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

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
  merge_context.static_param_.tables_handle_.add_table(handle1);
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
  merge_context.static_param_.tables_handle_.add_table(handle2);
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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, test_merge_with_multi_trans)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

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
  merge_context.static_param_.tables_handle_.add_table(handle1);
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
  merge_context.static_param_.tables_handle_.add_table(handle2);
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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
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
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

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
  merge_context.static_param_.tables_handle_.add_table(handle1);
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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, test_merge_with_multi_trans_can_not_compact)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

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
  merge_context.static_param_.tables_handle_.add_table(handle1);
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
  merge_context.static_param_.tables_handle_.add_table(handle2);
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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
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
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

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
  merge_context.static_param_.tables_handle_.add_table(handle1);
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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, test_merge_with_macro_reused_without_shadow)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

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
  merge_context.static_param_.tables_handle_.add_table(handle1);
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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, test_merge_with_greater_multi_version)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

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
  merge_context.static_param_.tables_handle_.add_table(handle1);
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
  merge_context.static_param_.tables_handle_.add_table(handle2);
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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, test_merge_with_greater_multi_version_and_uncommit)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

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
  merge_context.static_param_.tables_handle_.add_table(handle1);
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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, test_merge_with_ghost_row)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

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
  merge_context.static_param_.tables_handle_.add_table(handle1);
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
  merge_context.static_param_.tables_handle_.add_table(handle2);
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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, compare_dml_flag)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

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
  merge_context.static_param_.tables_handle_.add_table(handle1);
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
  merge_context.static_param_.tables_handle_.add_table(handle2);
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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, get_last_after_reuse)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

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

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "5        var5  -20      0        NOP    13    EXIST   LF\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}
TEST_F(TestMultiVersionMerge, rowkey_cross_two_macro_with_commit_scn_less_multi_version_start)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

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
  merge_context.static_param_.tables_handle_.add_table(handle1);
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
  merge_context.static_param_.tables_handle_.add_table(handle2);
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
      "2        var1  -8       0        3        2      EXIST   CLF\n"
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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, rowkey_cross_macro_with_last_shadow_version_less_than_multi_version)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

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
  merge_context.static_param_.tables_handle_.add_table(handle1);
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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, shadow_row_is_last_in_macro)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

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
  merge_context.static_param_.tables_handle_.add_table(handle1);
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
  merge_context.static_param_.tables_handle_.add_table(handle2);
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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, rowkey_cross_macro_without_open_next_macro)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

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
  merge_context.static_param_.tables_handle_.add_table(handle1);
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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, range_cross_macro)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

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
  merge_context.static_param_.tables_handle_.add_table(handle1);
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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, test_merge_base_iter_have_ghost_row)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

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
  merge_context.static_param_.tables_handle_.add_table(handle1);
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
  merge_context.static_param_.tables_handle_.add_table(handle2);
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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
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
  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag \n"
      "0        var0  -10      0        2       2     EXIST   N    \n"
      "1        var1  -30     -11      1       15    EXIST   N    \n"
      "2        var2  -20     -11      2       15    EXIST   N    \n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag \n"
      "3        var3  -20     0        25       25      EXIST  N \n"
      "4        var4  -20     0         25       8      EXIST  N  \n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag \n"
      "5        var5  -5        0        7        7      EXIST   N \n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 100;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version, MAJOR_MERGE);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 2);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);
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
  merge_context.static_param_.tables_handle_.add_table(handle2);
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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, test_trans_cross_macro_with_ghost_row)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag trans_id\n"
      "1        var1   MIN   0       NOP      6       EXIST   FU       trans_id_1\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag \n"
       "1        var1   MAGIC MAGIC   NOP     NOP     EXIST   LG\n"
       "2        var2   -26   0       7       NOP     EXIST   LF\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[2];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag trans_id\n"
       "1        var1  MIN     0       8       NOP    EXIST   ULF                  trans_id_2\n";

  micro_data2[1] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag \n"
      "3        var3  -46     0      18       NOP    EXIST   LF\n";

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
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
    "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag trans_id\n"
        "1        var1  -49     MIN      8        6      EXIST  SCF\n"
        "1        var1  -49     0      8        NOP      EXIST  \n"
      "1        var1  -29     0      NOP       6      EXIST   L\n"
      "2        var2  -26     0      7         NOP    EXIST   LF\n"
      "3        var3  -46     0      18        NOP    EXIST   LF\n";

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
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, test_trans_cross_macro_with_ghost_row2)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag trans_id\n"
      "1        var1  MIN     0       8       NOP    EXIST    FU                  trans_id_1\n"
      "1        var1  -17     MIN     9       5      EXIST    SC                trans_id_0\n"
      "1        var1  -17     0       9       5      EXIST    C                 trans_id_0\n"
      "1        var1  -12     0       9       NOP    EXIST    L                 trans_id_0\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag \n"
       "3        var3  -16     0      18       NOP    EXIST   LF\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[2];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag trans_id\n"
      "1        var1   MIN   0       NOP      6       EXIST   FU       trans_id_2\n";

  micro_data2[1] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag \n"
      "1        var1   MAGIC MAGIC   NOP     NOP     EXIST   LG\n"
      "2        var2   -26   0       7       NOP     EXIST   LF\n";

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
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
    "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1  -49     MIN      8         6      EXIST   SCF\n"
      "1        var1  -49     0      NOP       6      EXIST   N\n"
      "1        var1  -29     0      8         NOP      EXIST  N\n"
      "1        var1  -17     0      9         5      EXIST   CL\n"
      "2        var2  -26     0      7         NOP    EXIST   LF\n"
      "3        var3  -16     0      18        NOP    EXIST   LF\n";

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
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, test_running_trans_cross_macro_with_abort_sql_seq)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag trans_id\n"
      "1        var1  MIN     -10       8       NOP    EXIST    FU                trans_id_1\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  bigint flag    multi_version_row_flag trans_id\n"
      "1        var1  MIN     -8       8       NOP    EXIST    LU                trans_id_1\n"
      "3        var3  -16     0        18      NOP    EXIST    LF                trans_id_0\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[2];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0   -26   0       7       NOP     EXIST   LF\n"
      "2        var2   -26   0       7       NOP     EXIST   LF\n";

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
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
    "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag trans_id\n"
      "0        var0   -26    0         7       NOP    EXIST    LF                trans_id_0\n"
      "1        var1  MIN     -10       8       NOP    EXIST    FU                trans_id_1\n"
      "1        var1  MAX     MAX       NOP     NOP    EXIST    LG                trans_id_1\n"
      "2        var2  -26     0         7       NOP    EXIST    LF                trans_id_0\n"
      "3        var3  -16     0         18      NOP    EXIST    LF                trans_id_0\n";

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
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, check_shadow_row_fuse)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag trans_id\n"
      "0        var1  -8       0        NOP      1    T_DML_UPDATE  EXIST   LF        trans_id_0\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag trans_id\n"
      "2        var1  MIN      -9       2       2     T_DML_INSERT EXIST   U         trans_id_1\n"
      "2        var1  -6       0        3       2     T_DML_UPDATE EXIST   N         trans_id_0\n"
      "2        var1  MAGIC    MAGIC    NOP     NOP   T_DML_INSERT EXIST   LG        trans_id_0\n";

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
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  dml          flag    multi_version_row_flag\n"
      "2        var1  -10       0        NOP     12    T_DML_UPDATE EXIST   LF\n";

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
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "0        var1  -8       0        NOP      1      EXIST   LF\n"
      "2        var1  -10      MIN      3        12     EXIST   SCF\n"
      "2        var1  -10      0        NOP      12     EXIST   N\n"
      "2        var1  -6       0        3        2      EXIST   CL\n";

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
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, check_shadow_row_with_first)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag trans_id\n"
      "2        var1  MIN       0        10      1    T_DML_UPDATE  EXIST   FU        trans_id_1\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag trans_id\n"
      "2        var1  -20      MIN        2       2      T_DML_INSERT EXIST   SC         trans_id_0\n"
      "2        var1  -20      0        NOP     2      T_DML_UPDATE EXIST   N         trans_id_0\n"
      "2        var1  -10      0        2       NOP    T_DML_UPDATE EXIST   N         trans_id_0\n"
      "2        var1  -5       0        1       1      T_DML_UPDATE EXIST   CL         trans_id_0\n"
      "10       var1  -20      0        NOP    2   T_DML_UPDATE EXIST  LF         trans_id_0\n";

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
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  dml          flag    multi_version_row_flag\n"
      "10       var1  -30       0        2     12    T_DML_UPDATE EXIST   CLF\n";

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
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "2        var1  -20      MIN        2       2      EXIST   SCF\n"
      "2        var1  -20      0        NOP     2      EXIST   N\n"
      "2        var1  -10      0        2       NOP     EXIST   N\n"
      "2        var1  -5       0        1       1       EXIST   CL\n"
      "10       var1  -30       MIN        2     12     EXIST   SCF\n"
      "10       var1  -30       0        2     12    EXIST   C\n"
      "10       var1  -20      0        NOP    2   EXIST  L\n";

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
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, check_shadow_row_lost)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag trans_id\n"
      "0        var1  -20       0        10      1    T_DML_UPDATE  EXIST   CLF        trans_id_0\n";
  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag trans_id\n"
      "1        var1  -20       0        10      1    T_DML_UPDATE  EXIST   CLF        trans_id_0\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag trans_id\n"
      "2        var1  -20      MIN        2       2      T_DML_INSERT EXIST   SC         trans_id_0\n"
      "2        var1  -20      0        NOP     2      T_DML_UPDATE EXIST   N         trans_id_0\n"
      "2        var1  -10      0        2       NOP    T_DML_UPDATE EXIST   N         trans_id_0\n"
      "2        var1  -5       0        1       1      T_DML_UPDATE EXIST   CL         trans_id_0\n"
      "10       var1  -20      0        NOP    2   T_DML_UPDATE EXIST  LF         trans_id_0\n";

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
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  dml          flag    multi_version_row_flag\n"
      "10       var1  -30       0        2     12    T_DML_UPDATE EXIST   CLF\n";

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
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "0        var1  -20       0        10      1    EXIST   CLF\n"
      "1        var1  -20       0        10      1    EXIST   CLF\n"
      "2        var1  -20      MIN        2       2      EXIST   SC\n"
      "2        var1  -20      0        NOP     2      EXIST   N\n"
      "2        var1  -10      0        2       NOP     EXIST   N\n"
      "2        var1  -5       0        1       1       EXIST   CL\n"
      "10       var1  -30       MIN        2     12     EXIST   SCF\n"
      "10       var1  -30       0        2     12    EXIST   C\n"
      "10       var1  -20      0        NOP    2   EXIST  L\n";

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
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, single_trans_replayed_in_multi_sst)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

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
  merge_context.static_param_.tables_handle_.add_table(handle1);
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
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1  MIN     -89       NOP      9       EXIST   ULF   trans_id_5\n"
      "2        var2  MIN       -25     18       NOP     EXIST   ULF   trans_id_1\n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  ASSERT_EQ(OB_SUCCESS, clear_tx_data());
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  handle3.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMerge, across_multi_blocks)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data[200];
  micro_data[0] =
      "bigint      bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "1          -10       0        1        1    T_DML_INSERT  EXIST   CLF\n"
      "2          -10       0        2        2    T_DML_INSERT  EXIST   CLF\n";

  micro_data[1] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "3          -10      0        3       3     T_DML_INSERT EXIST   CLF\n";

  micro_data[2] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "4          -10      0        4       4     T_DML_INSERT EXIST   CLF\n";

  micro_data[3] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "5          -10      0        5       5     T_DML_INSERT EXIST   CLF\n";

  micro_data[4] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "6          -10      0        6       6     T_DML_INSERT EXIST   CLF\n";

  micro_data[5] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "7          -10      0        7       7     T_DML_INSERT EXIST   CLF\n";

  micro_data[6] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "8          -10      0        8       8     T_DML_INSERT EXIST   CLF\n";

  micro_data[7] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -100      MIN        100       100     T_DML_INSERT EXIST   SCF\n"
      "9          -100      0          NOP       100     T_DML_UPDATE EXIST   N\n";

  micro_data[8] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -99      0        NOP       99     T_DML_UPDATE EXIST   N\n";

  micro_data[9] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -98      0        NOP       98     T_DML_UPDATE EXIST   N\n";

  micro_data[10] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -97      0        NOP       97     T_DML_UPDATE EXIST   N\n";

  micro_data[11] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -96      0        NOP       96     T_DML_UPDATE EXIST   N\n";

  micro_data[12] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -95      0        NOP       95     T_DML_UPDATE EXIST   N\n";

  micro_data[13] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -94      0        NOP       94     T_DML_UPDATE EXIST   N\n";

  micro_data[14] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -93      0        NOP       93     T_DML_UPDATE EXIST   N\n";

  micro_data[15] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -92      0        NOP       92     T_DML_UPDATE EXIST   N\n";

  micro_data[16] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -91      0        NOP       91     T_DML_UPDATE EXIST   N\n";

  micro_data[17] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -90      0        NOP       90     T_DML_UPDATE EXIST   N\n";

  micro_data[18] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -89      0        NOP       89     T_DML_UPDATE EXIST   N\n";

  micro_data[19] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -88      0        NOP       88     T_DML_UPDATE EXIST   N\n";

  micro_data[20] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -87      0        NOP       87     T_DML_UPDATE EXIST   N\n";

  micro_data[21] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -86      0        NOP       86     T_DML_UPDATE EXIST   N\n";

  micro_data[22] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -85      0        NOP       85     T_DML_UPDATE EXIST   N\n";

  micro_data[23] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -84      0        NOP       84     T_DML_UPDATE EXIST   N\n";

  micro_data[24] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -83      0        NOP       83     T_DML_UPDATE EXIST   N\n";

  micro_data[25] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -82      0        NOP       82     T_DML_UPDATE EXIST   N\n";

  micro_data[26] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -81      0        NOP       81     T_DML_UPDATE EXIST   N\n";

  micro_data[27] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -80      0        NOP       80     T_DML_UPDATE EXIST   N\n";

  micro_data[28] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -79      0        NOP       79     T_DML_UPDATE EXIST   N\n";

  micro_data[29] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -78      0        NOP       78     T_DML_UPDATE EXIST   N\n";

  micro_data[30] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -77      0        NOP       77     T_DML_UPDATE EXIST   N\n";

  micro_data[31] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -76      0        NOP       76     T_DML_UPDATE EXIST   N\n";

  micro_data[32] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -75      0        NOP       75     T_DML_UPDATE EXIST   N\n";

  micro_data[33] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -74      0        NOP       74     T_DML_UPDATE EXIST   N\n";

  micro_data[34] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -73      0        NOP       73     T_DML_UPDATE EXIST   N\n";

  micro_data[35] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -72      0        NOP       72     T_DML_UPDATE EXIST   N\n";

  micro_data[36] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -71      0        NOP       71     T_DML_UPDATE EXIST   N\n";

  /*
  micro_data[37] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -70      0        NOP       70     T_DML_UPDATE EXIST   N\n";
      */

  micro_data[37] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9          -50      0        100       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[38] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "9           -18       0        68       NOP     T_DML_UPDATE EXIST   L\n"
      "13          -100      MIN      100       100     T_DML_UPDATE EXIST   SCF\n"
      "13          -100      0        NOP       100     T_DML_UPDATE EXIST   N\n";

  micro_data[40] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -99      0        NOP       99     T_DML_UPDATE EXIST   N\n";

  micro_data[41] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -98      0        NOP       98     T_DML_UPDATE EXIST   N\n";

  micro_data[42] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -97      0        NOP       97     T_DML_UPDATE EXIST   N\n";

  micro_data[43] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -96      0        NOP       96     T_DML_UPDATE EXIST   N\n";

  micro_data[44] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -95      0        NOP       95     T_DML_UPDATE EXIST   N\n";

  micro_data[45] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -94      0        NOP       94     T_DML_UPDATE EXIST   N\n";

  micro_data[46] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -93      0        NOP       93     T_DML_UPDATE EXIST   N\n";

  micro_data[47] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -92      0        NOP       92     T_DML_UPDATE EXIST   N\n";

  micro_data[48] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -91      0        NOP       91     T_DML_UPDATE EXIST   N\n";

  micro_data[49] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -90      0        NOP       90     T_DML_UPDATE EXIST   N\n";

  micro_data[50] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -89      0        NOP       89     T_DML_UPDATE EXIST   N\n";

  micro_data[51] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -88      0        NOP       88     T_DML_UPDATE EXIST   N\n";

  micro_data[52] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -87      0        NOP       87     T_DML_UPDATE EXIST   N\n";

  micro_data[53] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -86      0        NOP       86     T_DML_UPDATE EXIST   N\n";

  micro_data[54] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -85      0        NOP       85     T_DML_UPDATE EXIST   N\n";

  micro_data[55] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -84      0        NOP       84     T_DML_UPDATE EXIST   N\n";

  micro_data[56] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -83      0        NOP       83     T_DML_UPDATE EXIST   N\n";

  micro_data[57] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -82      0        NOP       82     T_DML_UPDATE EXIST   N\n";

  micro_data[58] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -81      0        NOP       81     T_DML_UPDATE EXIST   N\n";

  micro_data[59] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -80      0        NOP       80     T_DML_UPDATE EXIST   N\n";

  micro_data[60] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -79      0        NOP       79     T_DML_UPDATE EXIST   N\n";

  micro_data[61] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -78      0        NOP       78     T_DML_UPDATE EXIST   N\n";

  micro_data[62] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -77      0        NOP       77     T_DML_UPDATE EXIST   N\n";

  micro_data[63] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -76      0        NOP       76     T_DML_UPDATE EXIST   N\n";

  micro_data[64] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -75      0        NOP       75     T_DML_UPDATE EXIST   N\n";

  micro_data[65] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -74      0        NOP       74     T_DML_UPDATE EXIST   N\n";

  micro_data[66] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -73      0        NOP       73     T_DML_UPDATE EXIST   N\n";

  micro_data[67] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -72      0        NOP       72     T_DML_UPDATE EXIST   N\n";

  micro_data[68] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -71      0        NOP       71     T_DML_UPDATE EXIST   N\n";

  micro_data[69] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -70      0        NOP       70     T_DML_UPDATE EXIST   N\n";

  micro_data[70] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -50      0        100       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[71] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -49      0        99       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[72] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -48      0        98       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[73] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -47      0        97       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[74] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -46      0        96       NOP     T_DML_UPDATE EXIST  N\n";

  micro_data[75] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -45      0        95       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[76] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -44      0        94       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[77] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -43      0        93       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[78] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -42      0        92       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[79] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -41      0        91       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[80] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -40      0        90       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[81] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -39      0        89       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[82] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -38      0        88       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[83] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -37      0        87       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[84] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -36      0        86       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[85] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -35      0        85       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[86] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -34      0        84       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[87] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -33      0        83       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[88] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -32      0        82       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[89] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -31      0        81       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[90] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -30      0        80       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[91] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -29      0        79       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[92] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -28      0        78       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[93] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -27      0        77       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[94] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -26      0        76       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[95] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -25      0        75       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[96] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -24      0        74       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[97] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -23      0        73       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[98] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -22      0        72       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[99] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -21      0        71       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[100] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -20      0        70       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[101] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -19      0        69       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[102] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -18      0        68       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[103] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -17      0        67       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[104] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -16      0        66       NOP     T_DML_UPDATE EXIST   N\n";

  micro_data[105] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "13          -15      0        65       NOP     T_DML_UPDATE EXIST   CL\n"
      "15          -10      0        15       15     T_DML_UPDATE EXIST   CLF\n";

  micro_data[106] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "20          -10      0        20       20     T_DML_UPDATE EXIST   CLF\n";

  micro_data[107] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "21          -10      0        21       21     T_DML_UPDATE EXIST   CLF\n";

  micro_data[108] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "22          -10      0        22       22     T_DML_UPDATE EXIST   CLF\n";

  micro_data[109] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "23          -10      0        23       23     T_DML_UPDATE EXIST   CLF\n";

  micro_data[110] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "24          -10      0        24       24     T_DML_UPDATE EXIST   CLF\n";

  micro_data[111] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "25          -10      0        25       25     T_DML_UPDATE EXIST   CLF\n";

  micro_data[112] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "26          -10      0        26       26     T_DML_UPDATE EXIST   CLF\n";

  micro_data[113] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "27          -10      0        27       27     T_DML_UPDATE EXIST   CLF\n";

  micro_data[114] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "28          -10      0        28       28     T_DML_UPDATE EXIST   CLF\n";

  micro_data[115] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "29          -10      0        29       29     T_DML_UPDATE EXIST   CLF\n";

  micro_data[116] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "30          -10      0        30       30     T_DML_UPDATE EXIST   CLF\n";

  micro_data[117] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "31          -10      0        31       31     T_DML_UPDATE EXIST   CLF\n";

  micro_data[118] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "32          -10      0        32       32     T_DML_UPDATE EXIST   CLF\n";

  micro_data[119] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "33          -10      0        33       33     T_DML_UPDATE EXIST   CLF\n";

  micro_data[120] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "34          -10      0        34       34     T_DML_UPDATE EXIST   CLF\n";

  micro_data[121] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "35          -10      0        35       35     T_DML_UPDATE EXIST   CLF\n";

  micro_data[122] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "36          -10      0        36       36     T_DML_UPDATE EXIST   CLF\n";

  micro_data[123] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "37          -10      0        37       37     T_DML_UPDATE EXIST   CLF\n";

  micro_data[124] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "38          -10      0        38       38     T_DML_UPDATE EXIST   CLF\n";

  micro_data[125] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "39          -10      0        39       39     T_DML_UPDATE EXIST   CLF\n";

  micro_data[126] =
      "bigint      bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "40          -10      0        40       40     T_DML_UPDATE EXIST   CLF\n";
  int schema_rowkey_cnt = 1;

  int64_t snapshot_version = 100;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(100);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  /*
  prepare_one_macro(micro_data, 7);
  prepare_one_macro(&micro_data[7], 34);
  prepare_one_macro(&micro_data[41], 6);
  for (int64_t i = 0; i < 7; i++) {
    prepare_one_macro(micro_data+i, 1);
  }
  */
  prepare_one_macro(micro_data, 7);
  prepare_one_macro(micro_data + 7, 32);
  prepare_one_macro(micro_data + 40, 35);
  prepare_one_macro(micro_data + 75, 23);
  prepare_one_macro(micro_data + 98, 28);
  /*
  for (int64_t i = 134; i < 157; i++) {
    prepare_one_macro(micro_data+i, 1);
  }
  */

  prepare_data_end(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");
  table_store_iter.add_table(handle1.get_table());

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "1        1      1    INSERT    NORMAL\n"
      "2        2      2    INSERT    NORMAL\n"
      "3        3      3     INSERT    NORMAL\n"
      "4        4       4    INSERT    NORMAL\n"
      "5        5       5    INSERT    NORMAL\n"
      "6        6       6    INSERT    NORMAL\n"
      "7        7       7    INSERT    NORMAL\n"
      "8        8       8    INSERT    NORMAL\n"
      "9        100    100   INSERT    NORMAL\n"
      "13       100   100    INSERT    NORMAL\n"
      "20       20    20    INSERT    NORMAL\n"
      "21       21    21    INSERT    NORMAL\n"
      "22       22    22    INSERT    NORMAL\n"
      "23       23    23    INSERT    NORMAL\n"
      "24       24    24    INSERT    NORMAL\n";

  ObVersionRange trans_version_range;
  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  res_iter.reset();
  ObSEArray<blocksstable::ObDatumRange, 18> ranges;
  ObStorageDatum start[40];
  ObStorageDatum end[40];
  for (int64_t i = 0; i < 13; i++) {
    start[2*i].set_int(i+1);
    start[2*i+1].set_min();
    end[2*i].set_int(i+1);
    end[2*i+1].set_max();
    ObDatumRange range;
    range.start_key_.assign(start + 2 * i, 2);
    range.end_key_.assign(end + 2 * i, 2);
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(range));
  }

  for (int64_t i = 13; i < 18; i++) {
    start[2*i].set_int(i+7);
    start[2*i+1].set_min();
    end[2*i].set_int(i+7);
    end[2*i+1].set_max();
    ObDatumRange range;
    range.start_key_.assign(start + 2 * i, 2);
    range.end_key_.assign(end + 2 * i, 2);
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(range));
  }

  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter);
  ObMultipleMultiScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(ranges));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;

  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, 1);
    if (ret != OB_SUCCESS && ret != OB_ITER_END) {
      STORAGE_LOG(ERROR, "error return value", K(ret), K(count));
      ASSERT_EQ(1, 0);
    }
    if (count > 0) {
      ObMockScanMergeIterator merge_iter(count);
      ASSERT_EQ(OB_SUCCESS, merge_iter.init(reinterpret_cast<ObVectorStore *>(scan_merge.block_row_store_),
                                            query_allocator_, *access_param_.iter_param_.get_read_info()));
      bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
      ASSERT_TRUE(is_equal);

      total_count += count;
      STORAGE_LOG(INFO, "get next rows", K(count), K(total_count));
    } else {
      break;
    }
  }
  ASSERT_EQ(15, total_count);

  handle1.reset();
  scan_merge.reset();
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
