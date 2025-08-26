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
#include "storage/tx/ob_mock_tx_ctx.h"
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
#include "storage/init_basic_struct.h"
#include "storage/test_tablet_helper.h"
#include "storage/tx_table/ob_tx_table.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_trans_ctx_mgr_v4.h"
#include "storage/tx/ob_tx_data_define.h"
#include "share/scn.h"
#include "src/storage/column_store/ob_column_oriented_sstable.h"
#include "storage/column_store/ob_column_oriented_merger.h"
#include "storage/column_store/ob_co_merge_dag.h"
#include "unittest/storage/test_schema_prepare.h"
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

class ObMockWhiteFilterExecutor : public ObWhiteFilterExecutor
{
public:
  ObMockWhiteFilterExecutor(common::ObIAllocator &alloc,
                            ObPushdownWhiteFilterNode &filter,
                            ObPushdownOperator &op) :
      ObWhiteFilterExecutor(alloc, filter, op)
  {}

  virtual int init_evaluated_datums(bool &is_valid) override
  {
    UNUSED(is_valid);
    return OB_SUCCESS;
  };
};

class TestDeleteInsertRowScan : public TestMergeBasic
{
public:
  static const int64_t MAX_PARALLEL_DEGREE = 10;
  TestDeleteInsertRowScan();
  virtual ~TestDeleteInsertRowScan() {}

  void SetUp();
  void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
  void prepare_scan_param(const ObVersionRange &version_range,
                          const ObTableStoreIterator &table_store_iter);
  void prepare_output_expr(const ObIArray<int32_t> &projector,
                           const ObIArray<ObColDesc> &cols_desc);
  int create_pushdown_filter(const bool is_white,
                             const int64_t col_id,
                             const ObDatum &datum,
                             const ObWhiteFilterOperatorType &op_type,
                             const ObITableReadInfo &read_info,
                             ObPushdownFilterExecutor *&pushdown_filter);
  void prepare_txn(ObStoreCtx *store_ctx, const int64_t prepare_version);

  void fake_freeze_info();
  void get_tx_table_guard(ObTxTableGuard &tx_table_guard);

public:
  static const int64_t DATUM_ARRAY_CNT = 1024;
  static const int64_t DATUM_RES_SIZE = 10;
  static const int64_t SQL_BATCH_SIZE = 256;
  static ObLSTxCtxMgr ls_tx_ctx_mgr_;
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

ObLSTxCtxMgr TestDeleteInsertRowScan::ls_tx_ctx_mgr_;

void TestDeleteInsertRowScan::SetUpTestCase()
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

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  ls_handle.get_ls()->get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

}

void TestDeleteInsertRowScan::TearDownTestCase()
{
  clear_tx_data();
  ObMultiVersionSSTableTest::TearDownTestCase();
  ls_tx_ctx_mgr_.reset();
  ls_tx_ctx_mgr_.ls_tx_ctx_map_.reset();
  // reset sequence no
  ObClockGenerator::destroy();
}

TestDeleteInsertRowScan::TestDeleteInsertRowScan()
  : TestMergeBasic("test_delete_insert_row_scan"),
    exec_ctx_(query_allocator_),
    eval_ctx_(exec_ctx_),
    expr_spec_(query_allocator_),
    op_(eval_ctx_, expr_spec_)
{}

void TestDeleteInsertRowScan::SetUp()
{
  ObMultiVersionSSTableTest::SetUp();
}

void TestDeleteInsertRowScan::fake_freeze_info()
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

void TestDeleteInsertRowScan::TearDown()
{
  ObMultiVersionSSTableTest::TearDown();
  TRANS_LOG(INFO, "teardown success");
}

void TestDeleteInsertRowScan::prepare_txn(ObStoreCtx *store_ctx,
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

void TestDeleteInsertRowScan::prepare_scan_param(
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
  get_table_param_.tablet_iter_.tablet_handle_.assign(tablet_handle);
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
  access_param_.iter_param_.is_delete_insert_ = true;

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

void TestDeleteInsertRowScan::prepare_output_expr(
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

int TestDeleteInsertRowScan::create_pushdown_filter(
    const bool is_white,
    const int64_t col_id,
    const ObDatum &datum,
    const ObWhiteFilterOperatorType &op_type,
    const ObITableReadInfo &read_info,
    ObPushdownFilterExecutor *&pushdown_filter)
{
  int ret = OB_SUCCESS;
  ObMockWhiteFilterExecutor *filter = nullptr;
  ObIAllocator* allocator_ptr = &query_allocator_;
  ExprFixedArray *column_exprs = nullptr;
  if (!is_white) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "Not support to create not white filter", K(ret));
  } else {
    ObPushdownWhiteFilterNode* white_node =
        OB_NEWx(ObPushdownWhiteFilterNode, allocator_ptr, query_allocator_);
    filter = OB_NEWx(ObMockWhiteFilterExecutor, allocator_ptr, allocator_,
                     *white_node, op_);
    column_exprs = &(white_node->column_exprs_);
    white_node->op_type_ = op_type;
    pushdown_filter = filter;
  }

  if (OB_SUCC(ret)) {
    filter->null_param_contained_ = false;
    const common::ObIArray<ObColDesc> &cols_desc = read_info.get_columns_desc();
    const ObColumnIndexArray &cols_index = read_info.get_columns_index();
    if (OB_FAIL(column_exprs->init(1))) {
      STORAGE_LOG(WARN, "Fail to init column exprs", K(ret));
    } else if (OB_FAIL(column_exprs->push_back(nullptr))) {
      STORAGE_LOG(WARN, "Fail to push back col expr", K(ret));
    } else if (OB_FAIL(filter->filter_.col_ids_.init(1))) {
      STORAGE_LOG(WARN, "Fail to init col ids", K(ret));
    } else if (OB_FAIL(filter->filter_.col_ids_.push_back(col_id))) {
      STORAGE_LOG(WARN, "Fail to push back col id", K(ret));
    } else if (OB_FAIL(filter->datum_params_.init(1))) {
      STORAGE_LOG(WARN, "Fail to init datum params", K(ret));
    } else if (OB_FAIL(filter->datum_params_.push_back(datum))) {
      STORAGE_LOG(WARN, "Fail to push back datum", K(ret), K(datum));
    } else if (OB_FAIL(filter->cg_col_exprs_.init(1))) {
      STORAGE_LOG(WARN, "Fail to init cg col exprs", K(ret));
    } else if (OB_FAIL(filter->cg_col_exprs_.push_back(access_param_.output_exprs_->at(col_id - common::OB_APP_MIN_COLUMN_ID)))) {
      STORAGE_LOG(WARN, "Fail to push back col expr", K(ret));
    } else {
      filter->cmp_func_ = get_datum_cmp_func(cols_desc.at(col_id - common::OB_APP_MIN_COLUMN_ID).col_type_,
                                             cols_desc.at(col_id - common::OB_APP_MIN_COLUMN_ID).col_type_);
      STORAGE_LOG(INFO, "finish create pushdown filter");
    }
  }
  return ret;
}

void TestDeleteInsertRowScan::get_tx_table_guard(ObTxTableGuard &tx_table_guard)
{
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tx_table_guard(tx_table_guard));
}

TEST_F(TestDeleteInsertRowScan, test_row_scan)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "4        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "5        -70      MIN        99     99     INSERT    NORMAL        SCF\n"
      "5        -70      DI_VERSION 99     99     INSERT    NORMAL        C\n"
      "5        -70      0          19     19     DELETE    NORMAL        C\n"
      "5        -60      DI_VERSION 19     19     INSERT    NORMAL        C\n"
      "5        -60      0          9       9     DELETE    NORMAL        CL\n";

  snapshot_version = 100;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(100);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 50;

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "5        99      99   INSERT    NORMAL\n"
      "1        9       9    INSERT    NORMAL\n"
      "2        9       9    INSERT    NORMAL\n"
      "3        9       9    INSERT    NORMAL\n"
      "4        9       9    INSERT    NORMAL\n"
      "6        9       9    INSERT    NORMAL\n"
      "7        9       9    INSERT    NORMAL\n"
      "8        9       9    INSERT    NORMAL\n"
      "9        9       9    INSERT    NORMAL\n";

  ObMockIterator res_iter;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter);

  ObMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();
  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
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
  ASSERT_EQ(9, total_count);

  handle1.reset();
  handle2.reset();
  scan_merge.reset();
}

TEST_F(TestDeleteInsertRowScan, test_border_key)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "4        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 50;

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "1        9       9    INSERT    NORMAL\n"
      "2        9       9    INSERT    NORMAL\n"
      "3        9       9    INSERT    NORMAL\n"
      "4        9       9    INSERT    NORMAL\n"
      "6        9       9    INSERT    NORMAL\n"
      "7        9       9    INSERT    NORMAL\n"
      "8        9       9    INSERT    NORMAL\n"
      "9        9       9    INSERT    NORMAL\n";

  ObMockIterator res_iter;
  ObDatumRange range;
  res_iter.reset();
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter);

  ObDatumRowkey border_rowkey;
  void *buf = query_allocator_.alloc(sizeof(ObVectorStore));
  ObVectorStore *vector_store = new (buf) ObVectorStore(
      access_param_.get_op()->get_batch_size(),
      access_param_.get_op()->get_eval_ctx(),
      context_,
      nullptr);
  ASSERT_EQ(OB_SUCCESS, vector_store->init(access_param_));
  context_.block_row_store_ = vector_store;

  ObStoreRowIterator *iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, reinterpret_cast<ObSSTable *>(handle1.get_table())->scan(access_param_.iter_param_, context_, range, iter));
  ASSERT_NE(nullptr, iter);
  iter->block_row_store_ = vector_store;

  //1. test min
  border_rowkey.set_min_rowkey();
  vector_store->reuse_capacity(access_param_.get_op()->get_batch_size());
  ASSERT_EQ(OB_SUCCESS, iter->refresh_blockscan_checker(border_rowkey));
  ASSERT_EQ(OB_PUSHDOWN_STATUS_CHANGED, iter->get_next_rows());
  ASSERT_EQ(0, vector_store->get_row_count());

  //2. test rowkey = 5
  ObDatumRow delete_row;
  ASSERT_EQ(OB_SUCCESS, delete_row.init(query_allocator_, access_param_.iter_param_.get_read_info()->get_request_count()));
  delete_row.storage_datums_[0].set_int(5);
  ASSERT_EQ(OB_SUCCESS, border_rowkey.assign(delete_row.storage_datums_, access_param_.iter_param_.get_read_info()->get_schema_rowkey_count()));
  vector_store->reuse_capacity(access_param_.get_op()->get_batch_size());
  ASSERT_EQ(OB_SUCCESS, iter->refresh_blockscan_checker(border_rowkey));
  ASSERT_EQ(OB_PUSHDOWN_STATUS_CHANGED, iter->get_next_rows());
  ASSERT_EQ(4, vector_store->get_row_count());
  STORAGE_LOG(INFO, "get next rows", K(vector_store->get_row_count()));
  ObMockScanMergeIterator merge_iter(vector_store->get_row_count());
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(vector_store, query_allocator_, *access_param_.iter_param_.get_read_info()));
  bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
  ASSERT_TRUE(is_equal);

  // 3. test rowkey = max
  border_rowkey.set_max_rowkey();
  vector_store->reuse_capacity(access_param_.get_op()->get_batch_size());
  ASSERT_EQ(OB_SUCCESS, iter->refresh_blockscan_checker(border_rowkey));
  ASSERT_EQ(OB_SUCCESS, iter->get_next_rows());
  ASSERT_EQ(4, vector_store->get_row_count());
  STORAGE_LOG(INFO, "get next rows", K(vector_store->get_row_count()));
  ObMockScanMergeIterator merge_iter2(vector_store->get_row_count());
  ASSERT_EQ(OB_SUCCESS, merge_iter2.init(vector_store, query_allocator_, *access_param_.iter_param_.get_read_info()));
  is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter2, false, false, false, true);
  ASSERT_TRUE(is_equal);

  vector_store->reuse_capacity(access_param_.get_op()->get_batch_size());
  ASSERT_EQ(OB_ITER_END, iter->get_next_rows());

  vector_store->~ObVectorStore();
  query_allocator_.free(vector_store);
  iter->reset();
  handle1.reset();
}

TEST_F(TestDeleteInsertRowScan, test_border_key_reverse)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "4        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 50;

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "9        9       9    INSERT    NORMAL\n"
      "8        9       9    INSERT    NORMAL\n"
      "7        9       9    INSERT    NORMAL\n"
      "6        9       9    INSERT    NORMAL\n"
      "4        9       9    INSERT    NORMAL\n"
      "3        9       9    INSERT    NORMAL\n"
      "2        9       9    INSERT    NORMAL\n"
      "1        9       9    INSERT    NORMAL\n";

  ObMockIterator res_iter;
  ObDatumRange range;
  res_iter.reset();
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter);
  context_.query_flag_.scan_order_ = ObQueryFlag::Reverse;

  ObDatumRowkey border_rowkey;
  void *buf = query_allocator_.alloc(sizeof(ObVectorStore));
  ObVectorStore *vector_store = new (buf) ObVectorStore(
      access_param_.get_op()->get_batch_size(),
      access_param_.get_op()->get_eval_ctx(),
      context_,
      nullptr);
  ASSERT_EQ(OB_SUCCESS, vector_store->init(access_param_));
  context_.block_row_store_ = vector_store;

  ObStoreRowIterator *iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, reinterpret_cast<ObSSTable *>(handle1.get_table())->scan(access_param_.iter_param_, context_, range, iter));
  ASSERT_NE(nullptr, iter);
  iter->block_row_store_ = vector_store;

  //1. test rowkey = max
  border_rowkey.set_max_rowkey();
  vector_store->reuse_capacity(access_param_.get_op()->get_batch_size());
  ASSERT_EQ(OB_SUCCESS, iter->refresh_blockscan_checker(border_rowkey));
  ASSERT_EQ(OB_PUSHDOWN_STATUS_CHANGED, iter->get_next_rows());
  ASSERT_EQ(0, vector_store->get_row_count());

  //2. test rowkey = 5
  ObDatumRow delete_row;
  ASSERT_EQ(OB_SUCCESS, delete_row.init(query_allocator_, access_param_.iter_param_.get_read_info()->get_request_count()));
  delete_row.storage_datums_[0].set_int(5);
  ASSERT_EQ(OB_SUCCESS, border_rowkey.assign(delete_row.storage_datums_, access_param_.iter_param_.get_read_info()->get_schema_rowkey_count()));
  vector_store->reuse_capacity(access_param_.get_op()->get_batch_size());
  ASSERT_EQ(OB_SUCCESS, iter->refresh_blockscan_checker(border_rowkey));
  ASSERT_EQ(OB_PUSHDOWN_STATUS_CHANGED, iter->get_next_rows());
  ASSERT_EQ(4, vector_store->get_row_count());
  STORAGE_LOG(INFO, "get next rows", K(vector_store->get_row_count()));
  ObMockScanMergeIterator merge_iter(vector_store->get_row_count());
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(vector_store, query_allocator_, *access_param_.iter_param_.get_read_info()));
  bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
  ASSERT_TRUE(is_equal);

  // 3. test rowkey = min
  border_rowkey.set_min_rowkey();
  vector_store->reuse_capacity(access_param_.get_op()->get_batch_size());
  ASSERT_EQ(OB_SUCCESS, iter->refresh_blockscan_checker(border_rowkey));
  ASSERT_EQ(OB_SUCCESS, iter->get_next_rows());
  ASSERT_EQ(4, vector_store->get_row_count());
  STORAGE_LOG(INFO, "get next rows", K(vector_store->get_row_count()));
  ObMockScanMergeIterator merge_iter2(vector_store->get_row_count());
  ASSERT_EQ(OB_SUCCESS, merge_iter2.init(vector_store, query_allocator_, *access_param_.iter_param_.get_read_info()));
  is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter2, false, false, false, true);
  ASSERT_TRUE(is_equal);

  vector_store->reuse_capacity(access_param_.get_op()->get_batch_size());
  ASSERT_EQ(OB_ITER_END, iter->get_next_rows());

  vector_store->~ObVectorStore();
  query_allocator_.free(vector_store);
  iter->reset();
  handle1.reset();
}

TEST_F(TestDeleteInsertRowScan, test_border_key_not_equal)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "4        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "10        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "12        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 50;

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "2        9       9    INSERT    NORMAL\n"
      "4        9       9    INSERT    NORMAL\n"
      "6        9       9    INSERT    NORMAL\n"
      "8        9       9    INSERT    NORMAL\n"
      "10       9       9    INSERT    NORMAL\n"
      "12       9       9    INSERT    NORMAL\n";

  ObMockIterator res_iter;
  ObDatumRange range;
  res_iter.reset();
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter);

  ObDatumRowkey border_rowkey;
  void *buf = query_allocator_.alloc(sizeof(ObVectorStore));
  ObVectorStore *vector_store = new (buf) ObVectorStore(
      access_param_.get_op()->get_batch_size(),
      access_param_.get_op()->get_eval_ctx(),
      context_,
      nullptr);
  ASSERT_EQ(OB_SUCCESS, vector_store->init(access_param_));
  context_.block_row_store_ = vector_store;

  ObStoreRowIterator *iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, reinterpret_cast<ObSSTable *>(handle1.get_table())->scan(access_param_.iter_param_, context_, range, iter));
  ASSERT_NE(nullptr, iter);
  iter->block_row_store_ = vector_store;

  //1. test rowkey = min
  border_rowkey.set_min_rowkey();
  vector_store->reuse_capacity(access_param_.get_op()->get_batch_size());
  ASSERT_EQ(OB_SUCCESS, iter->refresh_blockscan_checker(border_rowkey));
  ASSERT_EQ(OB_PUSHDOWN_STATUS_CHANGED, iter->get_next_rows());
  ASSERT_EQ(0, vector_store->get_row_count());

  //2. test rowkey = 5
  ObDatumRow delete_row;
  ASSERT_EQ(OB_SUCCESS, delete_row.init(query_allocator_, access_param_.iter_param_.get_read_info()->get_request_count()));
  delete_row.storage_datums_[0].set_int(5);
  ASSERT_EQ(OB_SUCCESS, border_rowkey.assign(delete_row.storage_datums_, access_param_.iter_param_.get_read_info()->get_schema_rowkey_count()));
  vector_store->reuse_capacity(access_param_.get_op()->get_batch_size());
  ASSERT_EQ(OB_SUCCESS, iter->refresh_blockscan_checker(border_rowkey));
  ASSERT_EQ(OB_PUSHDOWN_STATUS_CHANGED, iter->get_next_rows());
  ASSERT_EQ(2, vector_store->get_row_count());
  STORAGE_LOG(INFO, "get next rows", K(vector_store->get_row_count()));
  ObMockScanMergeIterator merge_iter(vector_store->get_row_count());
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(vector_store, query_allocator_, *access_param_.iter_param_.get_read_info()));
  bool is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter, false, false, false, true);
  ASSERT_TRUE(is_equal);

  //3. test rowkey = 9
  delete_row.storage_datums_[0].set_int(9);
  ASSERT_EQ(OB_SUCCESS, border_rowkey.assign(delete_row.storage_datums_, access_param_.iter_param_.get_read_info()->get_schema_rowkey_count()));
  vector_store->reuse_capacity(access_param_.get_op()->get_batch_size());
  ASSERT_EQ(OB_SUCCESS, iter->refresh_blockscan_checker(border_rowkey));
  ASSERT_EQ(OB_PUSHDOWN_STATUS_CHANGED, iter->get_next_rows());
  ASSERT_EQ(2, vector_store->get_row_count());
  STORAGE_LOG(INFO, "get next rows", K(vector_store->get_row_count()));
  ObMockScanMergeIterator merge_iter2(vector_store->get_row_count());
  ASSERT_EQ(OB_SUCCESS, merge_iter2.init(vector_store, query_allocator_, *access_param_.iter_param_.get_read_info()));
  is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter2, false, false, false, true);
  ASSERT_TRUE(is_equal);

  // 3. test rowkey = max
  border_rowkey.set_max_rowkey();
  vector_store->reuse_capacity(access_param_.get_op()->get_batch_size());
  ASSERT_EQ(OB_SUCCESS, iter->refresh_blockscan_checker(border_rowkey));
  ASSERT_EQ(OB_SUCCESS, iter->get_next_rows());
  ASSERT_EQ(2, vector_store->get_row_count());
  STORAGE_LOG(INFO, "get next rows", K(vector_store->get_row_count()));
  ObMockScanMergeIterator merge_iter3(vector_store->get_row_count());
  ASSERT_EQ(OB_SUCCESS, merge_iter3.init(vector_store, query_allocator_, *access_param_.iter_param_.get_read_info()));
  is_equal = res_iter.equals<ObMockScanMergeIterator, ObStoreRow>(merge_iter3, false, false, false, true);
  ASSERT_TRUE(is_equal);

  vector_store->reuse_capacity(access_param_.get_op()->get_batch_size());
  ASSERT_EQ(OB_ITER_END, iter->get_next_rows());

  vector_store->~ObVectorStore();
  query_allocator_.free(vector_store);
  iter->reset();
  handle1.reset();
}

TEST_F(TestDeleteInsertRowScan, test_row_filter)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "4        -50      DI_VERSION    19      9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    99      9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 50;

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "4        19       9    INSERT    NORMAL\n"
      "6        99       9    INSERT    NORMAL\n";

  ObMockIterator res_iter;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter);
  ObStorageDatum filter_val;
  filter_val.set_int(9);
  // second column > 9
  ASSERT_EQ(OB_SUCCESS, create_pushdown_filter(true,
                                               common::OB_APP_MIN_COLUMN_ID + 1,
                                               filter_val,
                                               ObWhiteFilterOperatorType::WHITE_OP_GT,
                                               *access_param_.iter_param_.get_read_info(),
                                               access_param_.iter_param_.pushdown_filter_));

  ObMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();
  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
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
  ASSERT_EQ(2, total_count);

  handle1.reset();
  scan_merge.reset();
}

TEST_F(TestDeleteInsertRowScan, test_multi_version_row_filter)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;
  ObTableHandleV2 handle1;
  const char *micro_data1[3];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    1       1    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    2       2    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    3       3    INSERT    NORMAL      CLF\n"
      "4        -50      DI_VERSION    4       4    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    5       5    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    6       6    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    7       7    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    8       8    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";
  micro_data1[1] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "10       -50      DI_VERSION    10      10   INSERT    NORMAL      CLF\n"
      "11       -50      DI_VERSION    11      11   INSERT    NORMAL      CLF\n"
      "12       -50      DI_VERSION    12      12   INSERT    NORMAL      CLF\n"
      "13       -50      DI_VERSION    13      13   INSERT    NORMAL      CLF\n"
      "14       -50      DI_VERSION    14      14   INSERT    NORMAL      CLF\n"
      "15       -50      DI_VERSION    15      15   INSERT    NORMAL      CLF\n"
      "16       -50      DI_VERSION    16      16   INSERT    NORMAL      CLF\n"
      "17       -50      DI_VERSION    17      17   INSERT    NORMAL      CLF\n"
      "18       -50      DI_VERSION    18      18   INSERT    NORMAL      CLF\n";
  micro_data1[2] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "19       -50      DI_VERSION    19      19   INSERT    NORMAL      CLF\n"
      "20       -50      DI_VERSION    20      20   INSERT    NORMAL      CLF\n"
      "21       -50      DI_VERSION    21      21   INSERT    NORMAL      CLF\n"
      "22       -50      DI_VERSION    22      22   INSERT    NORMAL      CLF\n"
      "23       -50      DI_VERSION    23      23   INSERT    NORMAL      CLF\n"
      "24       -50      DI_VERSION    24      24   INSERT    NORMAL      CLF\n"
      "25       -50      DI_VERSION    25      25   INSERT    NORMAL      CLF\n"
      "26       -50      DI_VERSION    26      26   INSERT    NORMAL      CLF\n"
      "27       -50      DI_VERSION    27      27   INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 3);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable1");

  // case 1: return insert row without delete version
  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "5        -70      MIN        45     45     INSERT    NORMAL        SCF\n"
      "5        -70      DI_VERSION 45     45     INSERT    NORMAL        C\n"
      "5        -70      0          35     35     DELETE    NORMAL        C\n"
      "5        -60      DI_VERSION 35     35     INSERT    NORMAL        C\n"
      "5        -60      0          5       5     DELETE    NORMAL        CL\n";

  scn_range.start_scn_.convert_for_tx(60);
  scn_range.end_scn_.convert_for_tx(70);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  // case 2: return insert row with delete version
  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "13       -90      MIN        43     43     INSERT    NORMAL        SCF\n"
      "13       -90      DI_VERSION 43     43     INSERT    NORMAL        C\n"
      "13       -90      0          33     33     DELETE    NORMAL        C\n"
      "13       -80      DI_VERSION 33     33     INSERT    NORMAL        C\n"
      "13       -80      0          13     13     DELETE    NORMAL        CL\n";

  scn_range.start_scn_.convert_for_tx(70);
  scn_range.end_scn_.convert_for_tx(90);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  prepare_data_end(handle3);
  table_store_iter.add_table(handle3.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable3");

  // case 3: return delete row
  ObTableHandleV2 handle4;
  const char *micro_data4[1];
  micro_data4[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "26       -110     MIN        5      5      INSERT    NORMAL        SCF\n"
      "26       -110     DI_VERSION 5      5      INSERT    NORMAL        C\n"
      "26       -110     0          36     36     DELETE    NORMAL        C\n"
      "26       -100     DI_VERSION 36     36     INSERT    NORMAL        C\n"
      "26       -100     0          26     26     DELETE    NORMAL        CL\n";

  scn_range.start_scn_.convert_for_tx(90);
  scn_range.end_scn_.convert_for_tx(110);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data4, 1);
  prepare_data_end(handle4);
  table_store_iter.add_table(handle4.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable4");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 50;

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "5        45      45   INSERT    NORMAL\n"
      "13       43      43   INSERT    NORMAL\n"
      "1        1       1    INSERT    NORMAL\n"
      "2        2       2    INSERT    NORMAL\n"
      "3        3       3    INSERT    NORMAL\n"
      "4        4       4    INSERT    NORMAL\n"
      "6        6       6    INSERT    NORMAL\n"
      "7        7       7    INSERT    NORMAL\n"
      "8        8       8    INSERT    NORMAL\n"
      "9        9       9    INSERT    NORMAL\n"
      "10       10      10   INSERT    NORMAL\n"
      "11       11      11   INSERT    NORMAL\n"
      "12       12      12   INSERT    NORMAL\n"
      "14       14      14   INSERT    NORMAL\n"
      "15       15      15   INSERT    NORMAL\n"
      "16       16      16   INSERT    NORMAL\n"
      "17       17      17   INSERT    NORMAL\n"
      "18       18      18   INSERT    NORMAL\n"
      "19       19      19   INSERT    NORMAL\n"
      "20       20      20   INSERT    NORMAL\n"
      "21       21      21   INSERT    NORMAL\n"
      "22       22      22   INSERT    NORMAL\n"
      "23       23      23   INSERT    NORMAL\n"
      "24       24      24   INSERT    NORMAL\n"
      "25       25      25   INSERT    NORMAL\n"
      "27       27      27   INSERT    NORMAL\n";

  ObMockIterator res_iter;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter);
  ObStorageDatum filter_val;
  filter_val.set_int(5);
  // second column != 5
  OK(create_pushdown_filter(true,
                            common::OB_APP_MIN_COLUMN_ID + 1,
                            filter_val,
                            ObWhiteFilterOperatorType::WHITE_OP_NE,
                            *access_param_.iter_param_.get_read_info(),
                            access_param_.iter_param_.pushdown_filter_));
  ObMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();
  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
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
  ASSERT_EQ(26, total_count);
  handle1.reset();
  scan_merge.reset();
}

TEST_F(TestDeleteInsertRowScan, test_delete_scan)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "4        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "5        -60      0          9       9     DELETE    NORMAL        CLF\n"
      "6        -60      0          9       9     DELETE    NORMAL        CLF\n";

  snapshot_version = 100;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(100);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 50;

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "1        9       9    INSERT    NORMAL\n"
      "2        9       9    INSERT    NORMAL\n"
      "3        9       9    INSERT    NORMAL\n"
      "4        9       9    INSERT    NORMAL\n"
      "7        9       9    INSERT    NORMAL\n"
      "8        9       9    INSERT    NORMAL\n"
      "9        9       9    INSERT    NORMAL\n";

  ObMockIterator res_iter;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter);

  ObMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();
  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
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
  ASSERT_EQ(7, total_count);

  handle1.reset();
  handle2.reset();
  scan_merge.reset();
}

TEST_F(TestDeleteInsertRowScan, test_complex_update)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    10    10    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    20    20    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    30    30    INSERT    NORMAL      CLF\n"
      "4        -50      DI_VERSION    40    40    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    50    50    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    60    60    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    70    70    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    80    80    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    90    90    INSERT    NORMAL      CLF\n"
      "10       -50      DI_VERSION    100   100   INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -60      MIN         11    11     INSERT    NORMAL        SCF\n"
      "1        -60      DI_VERSION  11    11     INSERT    NORMAL        C\n"
      "1        -60      0           10    10     DELETE    NORMAL        CL\n"
      "2        -70      MIN         22    22     INSERT    NORMAL        SCF\n"
      "2        -70      DI_VERSION  22    22     INSERT    NORMAL        C\n"
      "2        -70      0           21    21     DELETE    NORMAL        C\n"
      "2        -60      DI_VERSION  21    21     INSERT    NORMAL        C\n"
      "2        -60      0           20    20     DELETE    NORMAL        CL\n"
      "3        -60      0           30    30     DELETE    NORMAL        CLF\n"
      "4        -70      MIN         41    41     INSERT    NORMAL        SCF\n"
      "4        -70      0           41    41     INSERT    NORMAL        C\n"
      "4        -60      0           40    40     DELETE    NORMAL        CL\n"
      "5        -80      MIN         52    52     INSERT    NORMAL        SCF\n"
      "5        -80      DI_VERSION  52    52     INSERT    NORMAL        C\n"
      "5        -80      0           51    51     DELETE    NORMAL        C\n"
      "5        -70      0           51    51     INSERT    NORMAL        C\n"
      "5        -60      0           50    50     DELETE    NORMAL        CL\n"
      "6        -90      MIN         63    63     INSERT    NORMAL        SCF\n"
      "6        -90      DI_VERSION  63    63     INSERT    NORMAL        C\n"
      "6        -90      0           62    62     DELETE    NORMAL        C\n"
      "6        -80      DI_VERSION  62    62     INSERT    NORMAL        C\n"
      "6        -80      0           61    61     DELETE    NORMAL        C\n"
      "6        -70      0           61    61     INSERT    NORMAL        C\n"
      "6        -60      0           60    60     DELETE    NORMAL        CL\n"
      "7        -70      MIN         71    71     DELETE    NORMAL        SCF\n"
      "7        -70      0           71    71     DELETE    NORMAL        C\n"
      "7        -60      DI_VERSION  71    71     INSERT    NORMAL        C\n"
      "7        -60      0           70    70     DELETE    NORMAL        CL\n"
      "8        -80      MIN         82    82     INSERT    NORMAL        SCF\n"
      "8        -80      0           82    82     INSERT    NORMAL        C\n"
      "8        -70      0           81    81     DELETE    NORMAL        C\n"
      "8        -60      DI_VERSION  81    81     INSERT    NORMAL        C\n"
      "8        -60      0           80    80     DELETE    NORMAL        CL\n"
      "9        -90      MIN         93    93     INSERT    NORMAL        SCF\n"
      "9        -90      DI_VERSION  93    93     INSERT    NORMAL        C\n"
      "9        -90      0           92    92     DELETE    NORMAL        C\n"
      "9        -80      0           92    92     INSERT    NORMAL        C\n"
      "9        -70      0           91    91     DELETE    NORMAL        C\n"
      "9        -60      DI_VERSION  91    91     INSERT    NORMAL        C\n"
      "9        -60      0           90    90     DELETE    NORMAL        CL\n"
      "10       -100     MIN         104   104    INSERT    NORMAL        SCF\n"
      "10       -100     DI_VERSION  104   104    INSERT    NORMAL        C\n"
      "10       -100     0           103   103    DELETE    NORMAL        C\n"
      "10       -90      DI_VERSION  103   103    INSERT    NORMAL        C\n"
      "10       -90      0           102   102    DELETE    NORMAL        C\n"
      "10       -80      0           102   102    INSERT    NORMAL        C\n"
      "10       -70      0           101   101    DELETE    NORMAL        C\n"
      "10       -60      DI_VERSION  101   101    INSERT    NORMAL        C\n"
      "10       -60      0           100   100    DELETE    NORMAL        CL\n";

  snapshot_version = 100;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(100);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 50;

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "1        11      11    INSERT    NORMAL\n"
      "2        22      22    INSERT    NORMAL\n"
      "4        41      41    INSERT    NORMAL\n"
      "5        52      52    INSERT    NORMAL\n"
      "6        63      63    INSERT    NORMAL\n"
      "8        82      82    INSERT    NORMAL\n"
      "9        93      93    INSERT    NORMAL\n"
      "10       104     104   INSERT    NORMAL\n";

  ObMockIterator res_iter;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  trans_version_range.base_version_ = 50;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter);

  ObMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();
  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
    STORAGE_LOG(INFO, "scan_merge.get_next_rows", K(ret), K(count), K(total_count));
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
    } else {
      break;
    }
  }
  ASSERT_EQ(8, total_count);

  handle1.reset();
  handle2.reset();
  scan_merge.reset();
}

TEST_F(TestDeleteInsertRowScan, test_delete_only)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    10    10    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    20    20    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    30    30    INSERT    NORMAL      CLF\n"
      "4        -50      DI_VERSION    40    40    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    50    50    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    60    60    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    70    70    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    80    80    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    90    90    INSERT    NORMAL      CLF\n"
      "10       -50      DI_VERSION    100   100   INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "3        -70      0          30     30     DELETE    NORMAL        CLF\n";

  snapshot_version = 100;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(100);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 50;

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "1        10      10    INSERT    NORMAL\n"
      "2        20      20    INSERT    NORMAL\n"
      "4        40      40    INSERT    NORMAL\n"
      "5        50      50    INSERT    NORMAL\n"
      "6        60      60    INSERT    NORMAL\n"
      "7        70      70    INSERT    NORMAL\n"
      "8        80      80    INSERT    NORMAL\n"
      "9        90      90    INSERT    NORMAL\n"
      "10       100     100   INSERT    NORMAL\n";

  ObMockIterator res_iter;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter);

  ObMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();
  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
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
  ASSERT_EQ(9, total_count);

  handle1.reset();
  handle2.reset();
  scan_merge.reset();
}

TEST_F(TestDeleteInsertRowScan, test_rowkey_cross_mb)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    10    10    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    20    20    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    30    30    INSERT    NORMAL      CLF\n"
      "4        -50      DI_VERSION    40    40    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    50    50    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    60    60    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    70    70    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    80    80    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    90    90    INSERT    NORMAL      CLF\n"
      "10       -50      DI_VERSION    100   100   INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[3];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -60      MIN         11    11     INSERT    NORMAL        SCF\n"
      "1        -60      DI_VERSION  11    11     INSERT    NORMAL        C\n"
      "1        -60      0           10    10     DELETE    NORMAL        CL\n"
      "2        -70      MIN         22    22     INSERT    NORMAL        SCF\n"
      "2        -70      DI_VERSION  22    22     INSERT    NORMAL        C\n"
      "2        -70      0           21    21     DELETE    NORMAL        C\n"
      "2        -60      DI_VERSION  21    21     INSERT    NORMAL        C\n"
      "2        -60      0           20    20     DELETE    NORMAL        CL\n"
      "3        -60      0           30    30     DELETE    NORMAL        CLF\n"
      "4        -70      MIN         41    41     INSERT    NORMAL        SCF\n"
      "4        -70      0           41    41     INSERT    NORMAL        C\n"
      "4        -60      0           40    40     DELETE    NORMAL        CL\n"
      "5        -80      MIN         52    52     INSERT    NORMAL        SCF\n"
      "5        -80      DI_VERSION  52    52     INSERT    NORMAL        C\n"
      "5        -80      0           51    51     DELETE    NORMAL        C\n"
      "5        -70      0           51    51     INSERT    NORMAL        C\n"
      "5        -60      0           50    50     DELETE    NORMAL        CL\n"
      "6        -90      MIN         63    63     INSERT    NORMAL        SCF\n"
      "6        -90      DI_VERSION  63    63     INSERT    NORMAL        C\n"
      "6        -90      0           62    62     DELETE    NORMAL        C\n";
  micro_data2[1] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "6        -80      DI_VERSION  62    62     INSERT    NORMAL        C\n"
      "6        -80      0           61    61     DELETE    NORMAL        C\n"
      "6        -70      0           61    61     INSERT    NORMAL        C\n"
      "6        -60      0           60    60     DELETE    NORMAL        CL\n"
      "7        -70      MIN         71    71     DELETE    NORMAL        SCF\n"
      "7        -70      0           71    71     DELETE    NORMAL        C\n"
      "7        -60      DI_VERSION  71    71     INSERT    NORMAL        C\n"
      "7        -60      0           70    70     DELETE    NORMAL        CL\n"
      "8        -80      MIN         82    82     INSERT    NORMAL        SCF\n"
      "8        -80      0           82    82     INSERT    NORMAL        C\n"
      "8        -70      0           81    81     DELETE    NORMAL        C\n"
      "8        -60      DI_VERSION  81    81     INSERT    NORMAL        C\n"
      "8        -60      0           80    80     DELETE    NORMAL        CL\n"
      "9        -90      MIN         93    93     INSERT    NORMAL        SCF\n"
      "9        -90      DI_VERSION  93    93     INSERT    NORMAL        C\n"
      "9        -90      0           92    92     DELETE    NORMAL        C\n"
      "9        -80      0           92    92     INSERT    NORMAL        C\n"
      "9        -70      0           91    91     DELETE    NORMAL        C\n";
  micro_data2[2] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "9        -60      DI_VERSION  91    91     INSERT    NORMAL        C\n"
      "9        -60      0           90    90     DELETE    NORMAL        CL\n"
      "10       -100     MIN         104   104    INSERT    NORMAL        SCF\n"
      "10       -100     DI_VERSION  104   104    INSERT    NORMAL        C\n"
      "10       -100     0           103   103    DELETE    NORMAL        C\n"
      "10       -90      DI_VERSION  103   103    INSERT    NORMAL        C\n"
      "10       -90      0           102   102    DELETE    NORMAL        C\n"
      "10       -80      0           102   102    INSERT    NORMAL        C\n"
      "10       -70      0           101   101    DELETE    NORMAL        C\n"
      "10       -60      DI_VERSION  101   101    INSERT    NORMAL        C\n"
      "10       -60      0           100   100    DELETE    NORMAL        CL\n";

  snapshot_version = 100;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(100);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 3);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 50;

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "1        11      11    INSERT    NORMAL\n"
      "2        22      22    INSERT    NORMAL\n"
      "4        41      41    INSERT    NORMAL\n"
      "5        52      52    INSERT    NORMAL\n"
      "6        63      63    INSERT    NORMAL\n"
      "8        82      82    INSERT    NORMAL\n"
      "9        93      93    INSERT    NORMAL\n"
      "10       104     104   INSERT    NORMAL\n";

  ObMockIterator res_iter;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  trans_version_range.base_version_ = 50;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter);

  ObMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();
  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
    STORAGE_LOG(INFO, "scan_merge.get_next_rows", K(ret), K(count), K(total_count));
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
    } else {
      break;
    }
  }
  ASSERT_EQ(8, total_count);

  handle1.reset();
  handle2.reset();
  scan_merge.reset();
}

TEST_F(TestDeleteInsertRowScan, test_one_mb_all_normal_rows)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    10    10    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    20    20    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    30    30    INSERT    NORMAL      CLF\n"
      "4        -50      DI_VERSION    40    40    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    60    60    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    80    80    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    90    90    INSERT    NORMAL      CLF\n"
      "10       -50      DI_VERSION    100   100   INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[3];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -70      MIN         11    11     INSERT    NORMAL        SCF\n"
      "1        -70      DI_VERSION  11    11     INSERT    NORMAL        C\n"
      "1        -70      0           10    10     DELETE    NORMAL        CL\n"
      "2        -70      MIN         21    21     INSERT    NORMAL        SCF\n"
      "2        -70      DI_VERSION  21    21     INSERT    NORMAL        C\n"
      "2        -70      0           20    20     DELETE    NORMAL        CL\n"
      "3        -70      0           30    30     DELETE    NORMAL        CLF\n";
  micro_data2[1] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "4        -70      0           40    40     DELETE    NORMAL        CLF\n"
      "5        -70      0           51    51     INSERT    NORMAL        CLF\n"
      "6        -70      0           60    60     DELETE    NORMAL        CLF\n"
      "7        -70      0           71    71     INSERT    NORMAL        CLF\n";
  micro_data2[2] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "8        -70      MIN         81    81     INSERT    NORMAL        SCF\n"
      "8        -70      DI_VERSION  81    81     INSERT    NORMAL        C\n"
      "8        -70      0           80    80     DELETE    NORMAL        CL\n"
      "9        -70      MIN         91    91     INSERT    NORMAL        SCF\n"
      "9        -70      DI_VERSION  91    91     INSERT    NORMAL        C\n"
      "9        -70      0           90    90     DELETE    NORMAL        CL\n"
      "10       -70      MIN         101   101    INSERT    NORMAL        SCF\n"
      "10       -70      DI_VERSION  101   101    INSERT    NORMAL        C\n"
      "10       -70      0           100   100    DELETE    NORMAL        CL\n";

  snapshot_version = 100;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(100);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 3);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 50;

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "1        11      11    INSERT    NORMAL\n"
      "2        21      21    INSERT    NORMAL\n"
      "5        51      51    INSERT    NORMAL\n"
      "7        71      71    INSERT    NORMAL\n"
      "8        81      81    INSERT    NORMAL\n"
      "9        91      91    INSERT    NORMAL\n"
      "10       101     101   INSERT    NORMAL\n";

  ObMockIterator res_iter;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  trans_version_range.base_version_ = 50;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter);

  ObMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();
  int64_t count = 0;
  int64_t total_count = 0;
  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
    STORAGE_LOG(INFO, "scan_merge.get_next_rows", K(ret), K(count), K(total_count));
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
    } else {
      break;
    }
  }
  ASSERT_EQ(7, total_count);

  handle1.reset();
  handle2.reset();
  scan_merge.reset();
}

TEST_F(TestDeleteInsertRowScan, test_cross_version_filter)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "11       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -80      MIN         19     9     INSERT    NORMAL        SCF\n"
      "1        -80      DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "1        -70      0           9      9     DELETE    NORMAL        CL\n"
      "2        -100      MIN        99     9     INSERT    NORMAL        SCF\n"
      "2        -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "2        -90      0           19     9     DELETE    NORMAL        C\n"
      "2        -80      DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "2        -70      0           9      9     DELETE    NORMAL        CL\n"
      "3        -80      DI_VERSION  19     9     INSERT    NORMAL        CF\n"
      "3        -70      0            9     9     DELETE    NORMAL        CL\n"
      "4        -80      DI_VERSION  19     9     INSERT    NORMAL        CLF\n"
      "5        -90      0           19     9     DELETE    NORMAL        CLF\n"
      "6        -90      0           19     9     DELETE    NORMAL        CLF\n"
      "7        -90      0           19     9     DELETE    NORMAL        CLF\n"
      "8        -90      0           19     9     DELETE    NORMAL        CLF\n"
      "9        -90      MIN         19     9     DELETE    NORMAL        SCF\n"
      "9        -90      0           19     9     DELETE    NORMAL        C\n"
      "9        -80      DI_VERSION  19     9     INSERT    NORMAL        CL\n"
      "10       -90      MIN         19     9     DELETE    NORMAL        SCF\n"
      "10       -90      0           19     9     DELETE    NORMAL        C\n"
      "10       -80      DI_VERSION  19     9     INSERT    NORMAL        CL\n"
      "11       -110     MIN         99     9     DELETE    NORMAL        SCF\n"
      "11       -110     0           99     9     DELETE    NORMAL        C\n"
      "11       -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "11       -90      0           19     9     DELETE    NORMAL        C\n"
      "11       -80      DI_VERSION  19     9     INSERT    NORMAL        CL\n"
      "12       -110      MIN        99     9     DELETE    NORMAL        SCF\n"
      "12       -110     0           99     9     DELETE    NORMAL        C\n"
      "12       -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "12       -90      0           19     9     DELETE    NORMAL        C\n"
      "12       -80      DI_VERSION  19     9     INSERT    NORMAL        CL\n";

  snapshot_version = 100;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(100);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -100      MIN        99     9     INSERT    NORMAL        SCF\n"
      "1        -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "1        -90      0           19     9     DELETE    NORMAL        C\n"
      "1        -80      DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "1        -70      0           9      9     DELETE    NORMAL        CL\n"
      "2        -100      MIN        99     9     INSERT    NORMAL        SCF\n"
      "2        -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "2        -90      0           19     9     DELETE    NORMAL        C\n"
      "2        -80      DI_VERSION  19     9     INSERT    NORMAL        CL\n"
      "3        -110     MIN         99     9     DELETE    NORMAL        SCF\n"
      "3        -110     0           99     9     DELETE    NORMAL        C\n"
      "3        -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "3        -90      0           19     9     DELETE    NORMAL        C\n"
      "3        -80      DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "3        -70      0            9     9     DELETE    NORMAL        CL\n"
      "4        -110      MIN        99     9     DELETE    NORMAL        SCF\n"
      "4        -110     0           99     9     DELETE    NORMAL        C\n"
      "4        -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "4        -90      0           19     9     DELETE    NORMAL        C\n"
      "4        -80      DI_VERSION  19     9     INSERT    NORMAL        CL\n"
      "5        -100      MIN        99     9     INSERT    NORMAL        SCF\n"
      "5        -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "5        -90      0           19     9     DELETE    NORMAL        C\n"
      "5        -80      DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "5        -70      0           9      9     DELETE    NORMAL        CL\n"
      "6        -100      MIN        99     9     INSERT    NORMAL        SCF\n"
      "6        -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "6        -90      0           19     9     DELETE    NORMAL        C\n"
      "6        -80      DI_VERSION  19     9     INSERT    NORMAL        CL\n"
      "7        -110     MIN         99     9     DELETE    NORMAL        SCF\n"
      "7        -110     0           99     9     DELETE    NORMAL        C\n"
      "7        -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "7        -90      0           19     9     DELETE    NORMAL        C\n"
      "7        -80      DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "7        -70      0            9     9     DELETE    NORMAL        CL\n"
      "8        -110      MIN        99     9     DELETE    NORMAL        SCF\n"
      "8        -110     0           99     9     DELETE    NORMAL        C\n"
      "8        -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "8        -90      0           19     9     DELETE    NORMAL        C\n"
      "8        -80      DI_VERSION  19     9     INSERT    NORMAL        CL\n"
      "9        -100      MIN        99     9     INSERT    NORMAL        SCF\n"
      "9        -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "9        -90      0           19     9     DELETE    NORMAL        C\n"
      "9        -80      DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "9        -70      0           9      9     DELETE    NORMAL        CL\n"
      "10       -100      MIN        99     9     INSERT    NORMAL        SCF\n"
      "10       -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "10       -90      0           19     9     DELETE    NORMAL        C\n"
      "10       -80      DI_VERSION  19     9     INSERT    NORMAL        CL\n"
      "11       -110     MIN         99     9     DELETE    NORMAL        SCF\n"
      "11       -110     0           99     9     DELETE    NORMAL        C\n"
      "11       -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "11       -90      0           19     9     DELETE    NORMAL        C\n"
      "11       -80      DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "11       -70      0            9     9     DELETE    NORMAL        CL\n"
      "12       -110      MIN        99     9     DELETE    NORMAL        SCF\n"
      "12       -110     0           99     9     DELETE    NORMAL        C\n"
      "12       -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "12       -90      0           19     9     DELETE    NORMAL        C\n"
      "12       -80      DI_VERSION  19     9     INSERT    NORMAL        CL\n";

  snapshot_version = 150;
  scn_range.start_scn_.convert_for_tx(100);
  scn_range.end_scn_.convert_for_tx(150);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  prepare_data_end(handle3);
  table_store_iter.add_table(handle3.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 50;

  ObDatumRange range;
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter);
  ObStorageDatum filter_val;
  filter_val.set_int(19);
  // second column = 19
  ASSERT_EQ(OB_SUCCESS, create_pushdown_filter(true,
                                               common::OB_APP_MIN_COLUMN_ID + 1,
                                               filter_val,
                                               ObWhiteFilterOperatorType::WHITE_OP_EQ,
                                               *access_param_.iter_param_.get_read_info(),
                                               access_param_.iter_param_.pushdown_filter_));

  ObMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();

  // 1. delete and insert row in the new sstable are filtered, and old sstable output insert row
  // 2. delete row in the new sstable is filtered, and old sstable output insert row
  // 3. insert row in the new sstable is filtered, and old sstable output insert row
  // 4. no delete and insert row in the new sstable, and old sstable output insert row
  // 5. delete and insert row in the new sstable are filtered, and old sstable output delete row
  // 6. delete row in the new sstable is filtered, and old sstable output delete row
  // 7. insert row in the new sstable is filtered, and old sstable output delete row
  // 8. no delete and insert row in the new sstable, and old sstable output delete row
  // 9. delete and insert row in the new sstable are filtered, and old sstable output earliest delete row
  // 10. delete row in the new sstable is filtered, and old sstable output earliest delete row
  // 11. insert row in the new sstable is filtered, and old sstable output earliest delete row
  // 12. no delete and insert row in the new sstable, and old sstable output earliest delete row
  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n";

  ret = OB_SUCCESS;
  int64_t count = 0;
  int64_t total_count = 0;
  ObMockIterator res_iter;
  res_iter.reset();
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
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
  ASSERT_EQ(0, total_count);

  handle1.reset();
  handle2.reset();
  handle3.reset();
  scan_merge.reset();
}

TEST_F(TestDeleteInsertRowScan, test_cross_version_partial_filter)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -80      MIN         19     9     INSERT    NORMAL        SCF\n"
      "1        -80      DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "1        -70      0           9      9     DELETE    NORMAL        CL\n"
      "5        -90      0           19     9     DELETE    NORMAL        CLF\n"
      "9        -90      MIN         19     9     DELETE    NORMAL        SCF\n"
      "9        -90      0           19     9     DELETE    NORMAL        C\n"
      "9        -80      DI_VERSION  19     9     INSERT    NORMAL        CL\n";

  snapshot_version = 100;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(100);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -100      MIN        19     9     INSERT    NORMAL        SCF\n"
      "1        -100     DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "1        -90      0           19     9     DELETE    NORMAL        C\n"
      "1        -80      DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "1        -70      0           9      9     DELETE    NORMAL        CL\n"
      "5        -100     MIN         19     9     INSERT    NORMAL        SCF\n"
      "5        -100     DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "5        -90      0           19     9     DELETE    NORMAL        C\n"
      "5        -80      DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "5        -70      0           9      9     DELETE    NORMAL        CL\n"
      "9        -100      MIN        19     9     INSERT    NORMAL        SCF\n"
      "9        -100     DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "9        -90      0           19     9     DELETE    NORMAL        C\n"
      "9        -80      DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "9        -70      0           9      9     DELETE    NORMAL        CL\n";

  snapshot_version = 150;
  scn_range.start_scn_.convert_for_tx(100);
  scn_range.end_scn_.convert_for_tx(150);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  prepare_data_end(handle3);
  table_store_iter.add_table(handle3.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = INT64_MAX;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 50;

  ObDatumRange range;
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter);
  ObStorageDatum filter_val;
  filter_val.set_int(19);
  // second column = 19
  ASSERT_EQ(OB_SUCCESS, create_pushdown_filter(true,
                                               common::OB_APP_MIN_COLUMN_ID + 1,
                                               filter_val,
                                               ObWhiteFilterOperatorType::WHITE_OP_EQ,
                                               *access_param_.iter_param_.get_read_info(),
                                               access_param_.iter_param_.pushdown_filter_));

  ObMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "1        19       9    INSERT    NORMAL\n"
      "5        19       9    INSERT    NORMAL\n"
      "9        19       9    INSERT    NORMAL\n";

  ret = OB_SUCCESS;
  int64_t count = 0;
  int64_t total_count = 0;
  ObMockIterator res_iter;
  res_iter.reset();
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
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
  ASSERT_EQ(3, total_count);

  handle1.reset();
  handle2.reset();
  handle3.reset();
  scan_merge.reset();
}

TEST_F(TestDeleteInsertRowScan, test_cross_version_without_filter)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "11       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -80      MIN         19     9     INSERT    NORMAL        SCF\n"
      "1        -80      DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "1        -70      0           9      9     DELETE    NORMAL        CL\n"
      "2        -100      MIN        99     9     INSERT    NORMAL        SCF\n"
      "2        -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "2        -90      0           19     9     DELETE    NORMAL        C\n"
      "2        -80      DI_VERSION  19     9     INSERT    NORMAL        CL\n"
      "3        -80      DI_VERSION  19     9     INSERT    NORMAL        CF\n"
      "3        -70      0            9     9     DELETE    NORMAL        CL\n"
      "4        -80      DI_VERSION  19     9     INSERT    NORMAL        CLF\n"
      "5        -90      0           19     9     DELETE    NORMAL        CLF\n"
      "6        -90      0           19     9     DELETE    NORMAL        CLF\n"
      "7        -90      0           19     9     DELETE    NORMAL        CLF\n"
      "8        -90      0           19     9     DELETE    NORMAL        CLF\n"
      "9        -90      MIN         19     9     DELETE    NORMAL        SCF\n"
      "9        -90      0           19     9     DELETE    NORMAL        C\n"
      "9        -80      DI_VERSION  19     9     INSERT    NORMAL        CL\n"
      "10       -90      MIN         19     9     DELETE    NORMAL        SCF\n"
      "10       -90      0           19     9     DELETE    NORMAL        C\n"
      "10       -80      DI_VERSION  19     9     INSERT    NORMAL        CL\n"
      "11       -110     MIN         99     9     DELETE    NORMAL        SCF\n"
      "11       -110     0           99     9     DELETE    NORMAL        C\n"
      "11       -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "11       -90      0           19     9     DELETE    NORMAL        C\n"
      "11       -80      DI_VERSION  19     9     INSERT    NORMAL        CL\n"
      "12       -110      MIN        99     9     DELETE    NORMAL        SCF\n"
      "12       -110     0           99     9     DELETE    NORMAL        C\n"
      "12       -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "12       -90      0           19     9     DELETE    NORMAL        C\n"
      "12       -80      DI_VERSION  19     9     INSERT    NORMAL        CL\n";

  snapshot_version = 100;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(100);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -100      MIN        99     9     INSERT    NORMAL        SCF\n"
      "1        -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "1        -90      0           19     9     DELETE    NORMAL        C\n"
      "1        -80      DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "1        -70      0           9      9     DELETE    NORMAL        CL\n"
      "2        -100      MIN        99     9     INSERT    NORMAL        SCF\n"
      "2        -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "2        -90      0           19     9     DELETE    NORMAL        C\n"
      "2        -80      DI_VERSION  19     9     INSERT    NORMAL        CL\n"
      "3        -110     MIN         99     9     DELETE    NORMAL        SCF\n"
      "3        -110     0           99     9     DELETE    NORMAL        C\n"
      "3        -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "3        -90      0           19     9     DELETE    NORMAL        C\n"
      "3        -80      DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "3        -70      0            9     9     DELETE    NORMAL        CL\n"
      "4        -110      MIN        99     9     DELETE    NORMAL        SCF\n"
      "4        -110     0           99     9     DELETE    NORMAL        C\n"
      "4        -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "4        -90      0           19     9     DELETE    NORMAL        C\n"
      "4        -80      DI_VERSION  19     9     INSERT    NORMAL        CL\n"
      "5        -100      MIN        99     9     INSERT    NORMAL        SCF\n"
      "5        -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "5        -90      0           19     9     DELETE    NORMAL        C\n"
      "5        -80      DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "5        -70      0           9      9     DELETE    NORMAL        CL\n"
      "6        -100      MIN        99     9     INSERT    NORMAL        SCF\n"
      "6        -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "6        -90      0           19     9     DELETE    NORMAL        C\n"
      "6        -80      DI_VERSION  19     9     INSERT    NORMAL        CL\n"
      "7        -110     MIN         99     9     DELETE    NORMAL        SCF\n"
      "7        -110     0           99     9     DELETE    NORMAL        C\n"
      "7        -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "7        -90      0           19     9     DELETE    NORMAL        C\n"
      "7        -80      DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "7        -70      0            9     9     DELETE    NORMAL        CL\n"
      "8        -110      MIN        99     9     DELETE    NORMAL        SCF\n"
      "8        -110     0           99     9     DELETE    NORMAL        C\n"
      "8        -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "8        -90      0           19     9     DELETE    NORMAL        C\n"
      "8        -80      DI_VERSION  19     9     INSERT    NORMAL        CL\n"
      "9        -100      MIN        99     9     INSERT    NORMAL        SCF\n"
      "9        -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "9        -90      0           19     9     DELETE    NORMAL        C\n"
      "9        -80      DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "9        -70      0           9      9     DELETE    NORMAL        CL\n"
      "10       -100      MIN        99     9     INSERT    NORMAL        SCF\n"
      "10       -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "10       -90      0           19     9     DELETE    NORMAL        C\n"
      "10       -80      DI_VERSION  19     9     INSERT    NORMAL        CL\n"
      "11       -110     MIN         99     9     DELETE    NORMAL        SCF\n"
      "11       -110     0           99     9     DELETE    NORMAL        C\n"
      "11       -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "11       -90      0           19     9     DELETE    NORMAL        C\n"
      "11       -80      DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "11       -70      0            9     9     DELETE    NORMAL        CL\n"
      "12       -110      MIN        99     9     DELETE    NORMAL        SCF\n"
      "12       -110     0           99     9     DELETE    NORMAL        C\n"
      "12       -100     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "12       -90      0           19     9     DELETE    NORMAL        C\n"
      "12       -80      DI_VERSION  19     9     INSERT    NORMAL        CL\n";

  snapshot_version = 150;
  scn_range.start_scn_.convert_for_tx(100);
  scn_range.end_scn_.convert_for_tx(150);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  prepare_data_end(handle3);
  table_store_iter.add_table(handle3.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObDatumRange range;
  range.set_whole_range();
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_scan_param(trans_version_range, table_store_iter);

  ObMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "1        99       9    INSERT    NORMAL\n"
      "2        99       9    INSERT    NORMAL\n"
      "5        99       9    INSERT    NORMAL\n"
      "6        99       9    INSERT    NORMAL\n"
      "9        99       9    INSERT    NORMAL\n"
      "10       99       9    INSERT    NORMAL\n";

  ret = OB_SUCCESS;
  int64_t count = 0;
  int64_t total_count = 0;
  ObMockIterator res_iter;
  res_iter.reset();
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
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
  ASSERT_EQ(6, total_count);

  handle1.reset();
  handle2.reset();
  handle3.reset();
  scan_merge.reset();
}

TEST_F(TestDeleteInsertRowScan, test_multi_sstables_cross_version)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  // major sstable
  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "4        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "9        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "10       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "11       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "12       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "13       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "14       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "15       -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";
  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable1");

  // minor sstable 1
  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "6        -80      MIN         19     9     INSERT    NORMAL        SCF\n"
      "6        -80      DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "6        -70      0           9      9     DELETE    NORMAL        CL\n"
      "12       -100     MIN          9     9     INSERT    NORMAL        SCF\n"
      "12       -100     DI_VERSION   9     9     INSERT    NORMAL        C\n"
      "12       -90      0           19     9     DELETE    NORMAL        C\n"
      "12       -80      DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "12       -70      0           9      9     DELETE    NORMAL        CL\n";
  snapshot_version = 100;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(100);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  // minor sstable 2, cross range
  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "6        -110     MIN         99     9     INSERT    NORMAL        SCF\n"
      "6        -110     0           19     9     DELETE    NORMAL        C\n"
      "6        -100     DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "6        -90      0           9      9     DELETE    NORMAL        CL\n"
      "12       -150      MIN        99     9     INSERT    NORMAL        SCF\n"
      "12       -150     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "12       -140     0           19     9     DELETE    NORMAL        C\n"
      "12       -130     DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "12       -120     0           9      9     DELETE    NORMAL        CL\n";

  snapshot_version = 150;
  scn_range.start_scn_.convert_for_tx(100);
  scn_range.end_scn_.convert_for_tx(150);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 1);
  prepare_data_end(handle3);
  table_store_iter.add_table(handle3.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable3");

  // minor sstable 3
  ObTableHandleV2 handle4;
  const char *micro_data4[1];
  micro_data4[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "6        -110     MIN         9      9     INSERT    NORMAL        SCF\n"
      "6        -110     DI_VERSION  9      9     INSERT    NORMAL        C\n"
      "6        -110     0           19     9     DELETE    NORMAL        C\n"
      "6        -100     DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "6        -90      0           9      9     DELETE    NORMAL        CL\n"
      "12       -150      MIN        99     9     INSERT    NORMAL        SCF\n"
      "12       -150     DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "12       -140     0           19     9     DELETE    NORMAL        C\n"
      "12       -130     DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "12       -120     0           9      9     DELETE    NORMAL        CL\n";

  snapshot_version = 200;
  scn_range.start_scn_.convert_for_tx(150);
  scn_range.end_scn_.convert_for_tx(200);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data4, 1);
  prepare_data_end(handle4);
  table_store_iter.add_table(handle4.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable4");

  ObDatumRange range;
  range.set_whole_range();
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = 110;
  prepare_scan_param(trans_version_range, table_store_iter);
  ObStorageDatum filter_val;
  filter_val.set_int(9);
  // second column = 9
  ASSERT_EQ(OB_SUCCESS, create_pushdown_filter(true,
                                               common::OB_APP_MIN_COLUMN_ID + 1,
                                               filter_val,
                                               ObWhiteFilterOperatorType::WHITE_OP_EQ,
                                               *access_param_.iter_param_.get_read_info(),
                                               access_param_.iter_param_.pushdown_filter_));

  ObMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "6         9       9    INSERT    NORMAL\n"
      "1         9       9    INSERT    NORMAL\n"
      "2         9       9    INSERT    NORMAL\n"
      "3         9       9    INSERT    NORMAL\n"
      "4         9       9    INSERT    NORMAL\n"
      "5         9       9    INSERT    NORMAL\n"
      "12        9       9    INSERT    NORMAL\n"
      "7         9       9    INSERT    NORMAL\n"
      "8         9       9    INSERT    NORMAL\n"
      "9         9       9    INSERT    NORMAL\n"
      "10        9       9    INSERT    NORMAL\n"
      "11        9       9    INSERT    NORMAL\n"
      "13        9       9    INSERT    NORMAL\n"
      "14        9       9    INSERT    NORMAL\n"
      "15        9       9    INSERT    NORMAL\n";

  ret = OB_SUCCESS;
  int64_t count = 0;
  int64_t total_count = 0;
  ObMockIterator res_iter;
  res_iter.reset();
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
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
  handle2.reset();
  handle3.reset();
  handle4.reset();
  scan_merge.reset();
}

TEST_F(TestDeleteInsertRowScan, test_crossed_minor_and_major)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    19      9    INSERT    NORMAL      CLF\n"
      "4        -50      DI_VERSION    19      9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    19      9    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    19      9    INSERT    NORMAL      CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -80      MIN         19     9     INSERT    NORMAL        SCF\n"
      "2        -80      DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "2        -70      0           9      9     DELETE    NORMAL        C\n"
      "2        -50      DI_VERSION  9      9     INSERT    NORMAL        CL\n"
      "4        -90       MIN        19     9     DELETE    NORMAL        SCF\n"
      "4        -90      0           19     9     DELETE    NORMAL        C\n"
      "4        -50      DI_VERSION  19     9     INSERT    NORMAL        CL\n"
      "6        -80      MIN         19     9     INSERT    NORMAL        SCF\n"
      "6        -80      DI_VERSION  19     9     INSERT    NORMAL        C\n"
      "6        -70      0           9      9     DELETE    NORMAL        C\n"
      "6        -50      DI_VERSION  9      9     INSERT    NORMAL        CL\n"
      "8        -90      MIN         19     9     DELETE    NORMAL        SCF\n"
      "8        -90      0           19     9     DELETE    NORMAL        C\n"
      "8        -50      DI_VERSION  19     9     INSERT    NORMAL        CL\n";
  snapshot_version = 100;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(100);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObDatumRange range;
  range.set_whole_range();
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 50;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = 110;
  prepare_scan_param(trans_version_range, table_store_iter);
  ObStorageDatum filter_val;
  filter_val.set_int(19);
  // second column = 19
  ASSERT_EQ(OB_SUCCESS, create_pushdown_filter(true,
                                               common::OB_APP_MIN_COLUMN_ID + 1,
                                               filter_val,
                                               ObWhiteFilterOperatorType::WHITE_OP_EQ,
                                               *access_param_.iter_param_.get_read_info(),
                                               access_param_.iter_param_.pushdown_filter_));

  ObMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "2         19      9    INSERT    NORMAL\n"
      "3         19      9    INSERT    NORMAL\n"
      "6         19      9    INSERT    NORMAL\n"
      "7         19      9    INSERT    NORMAL\n";

  ret = OB_SUCCESS;
  int64_t count = 0;
  int64_t total_count = 0;
  ObMockIterator res_iter;
  res_iter.reset();
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
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
  ASSERT_EQ(4, total_count);

  handle1.reset();
  handle2.reset();
  scan_merge.reset();
}

TEST_F(TestDeleteInsertRowScan, test_filtered_row_and_blockscan)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "4        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "5        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "6        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "7        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n"
      "8        -50      DI_VERSION    9       9    INSERT    NORMAL      CLF\n";
  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(50);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  table_store_iter.add_table(handle1.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -80      DI_VERSION  9      9     INSERT    NORMAL        CLF\n"
      "2        -80      MIN         99     9     INSERT    NORMAL        SCF\n"
      "2        -80      DI_VERSION  99     9     INSERT    NORMAL        C\n"
      "2        -70      0           9      9     DELETE    NORMAL        CL\n";
  snapshot_version = 100;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(100);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObDatumRange range;
  range.set_whole_range();
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 50;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = 110;
  prepare_scan_param(trans_version_range, table_store_iter);
  ObStorageDatum filter_val;
  filter_val.set_int(9);
  // second column = 9
  ASSERT_EQ(OB_SUCCESS, create_pushdown_filter(true,
                                               common::OB_APP_MIN_COLUMN_ID + 1,
                                               filter_val,
                                               ObWhiteFilterOperatorType::WHITE_OP_EQ,
                                               *access_param_.iter_param_.get_read_info(),
                                               access_param_.iter_param_.pushdown_filter_));

  ObMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "1         9       9    INSERT    NORMAL\n"
      "3         9       9    INSERT    NORMAL\n"
      "4         9       9    INSERT    NORMAL\n"
      "5         9       9    INSERT    NORMAL\n"
      "6         9       9    INSERT    NORMAL\n"
      "7         9       9    INSERT    NORMAL\n"
      "8         9       9    INSERT    NORMAL\n";

  ret = OB_SUCCESS;
  int64_t count = 0;
  int64_t total_count = 0;
  ObMockIterator res_iter;
  res_iter.reset();
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  while (OB_SUCC(ret)) {
    ret = scan_merge.get_next_rows(count, SQL_BATCH_SIZE);
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
  ASSERT_EQ(7, total_count);

  handle1.reset();
  handle2.reset();
  scan_merge.reset();
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_delete_insert_row_scan.log*");
  OB_LOGGER.set_file_name("test_delete_insert_row_scan.log");
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
