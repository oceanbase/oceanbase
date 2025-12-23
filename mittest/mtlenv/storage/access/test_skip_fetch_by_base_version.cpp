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

class TestSkipFetchByBaseVersion : public TestMergeBasic
{
public:
  static const int64_t MAX_PARALLEL_DEGREE = 10;
  TestSkipFetchByBaseVersion();
  virtual ~TestSkipFetchByBaseVersion() {}

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

ObLSTxCtxMgr TestSkipFetchByBaseVersion::ls_tx_ctx_mgr_;

void TestSkipFetchByBaseVersion::SetUpTestCase()
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

void TestSkipFetchByBaseVersion::TearDownTestCase()
{
  clear_tx_data();
  ObMultiVersionSSTableTest::TearDownTestCase();
  ls_tx_ctx_mgr_.reset();
  ls_tx_ctx_mgr_.ls_tx_ctx_map_.reset();
  // reset sequence no
  ObClockGenerator::destroy();
}

TestSkipFetchByBaseVersion::TestSkipFetchByBaseVersion()
  : TestMergeBasic("test_skip_fetch_by_base_version"),
    exec_ctx_(query_allocator_),
    eval_ctx_(exec_ctx_),
    expr_spec_(query_allocator_),
    op_(eval_ctx_, expr_spec_)
{}

void TestSkipFetchByBaseVersion::SetUp()
{
  ObMultiVersionSSTableTest::SetUp();
}

void TestSkipFetchByBaseVersion::fake_freeze_info()
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

void TestSkipFetchByBaseVersion::TearDown()
{
  ObMultiVersionSSTableTest::TearDown();
  TRANS_LOG(INFO, "teardown success");
}

void TestSkipFetchByBaseVersion::prepare_txn(ObStoreCtx *store_ctx,
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

void TestSkipFetchByBaseVersion::prepare_scan_param(
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

void TestSkipFetchByBaseVersion::prepare_output_expr(
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

int TestSkipFetchByBaseVersion::create_pushdown_filter(
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

void TestSkipFetchByBaseVersion::get_tx_table_guard(ObTxTableGuard &tx_table_guard)
{
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tx_table_guard(tx_table_guard));
}

TEST_F(TestSkipFetchByBaseVersion, test_skip_mb)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    10      10    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    20      20    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    30      30    INSERT    NORMAL      CLF\n";

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
      "1        -40      MIN        11     11     INSERT    NORMAL        SCF\n"
      "1        -40      DI_VERSION 11     11     INSERT    NORMAL        C\n"
      "1        -30      0          10     10     DELETE    NORMAL        CL\n"
      "2        -30      MIN        21     21     INSERT    NORMAL        SCF\n"
      "2        -30      DI_VERSION 21     21     INSERT    NORMAL        C\n"
      "2        -30      0          20     20     DELETE    NORMAL        CL\n"
      "3        -40      MIN        31     31     INSERT    NORMAL        SCF\n"
      "3        -40      DI_VERSION 31     31     INSERT    NORMAL        C\n"
      "3        -30      0          30     30     DELETE    NORMAL        CL\n";

  snapshot_version = 100;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "1        10      10    INSERT    NORMAL\n"
      "2        20      20    INSERT    NORMAL\n"
      "3        30      30    INSERT    NORMAL\n";

  ObMockIterator res_iter;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  ObVersionRange trans_version_range;
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
  scan_merge.reset();
}

TEST_F(TestSkipFetchByBaseVersion, test_not_skip_mb)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    10      10    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    20      20    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    30      30    INSERT    NORMAL      CLF\n";

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
      "1        -30      MIN        11     11     INSERT    NORMAL        SCF\n"
      "1        -30      DI_VERSION 11     11     INSERT    NORMAL        C\n"
      "1        -30      0          10     10     DELETE    NORMAL        CL\n"
      "2        -30      MIN        21     21     INSERT    NORMAL        SCF\n"
      "2        -30      DI_VERSION 21     21     INSERT    NORMAL        C\n"
      "2        -30      0          20     20     DELETE    NORMAL        CL\n"
      "3        -60      MIN        31     31     INSERT    NORMAL        SCF\n"
      "3        -60      DI_VERSION 31     31     INSERT    NORMAL        C\n"
      "3        -60      0          30     30     DELETE    NORMAL        CL\n";

  snapshot_version = 100;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "3        31      31    INSERT    NORMAL\n"
      "1        10      10    INSERT    NORMAL\n"
      "2        20      20    INSERT    NORMAL\n";

  ObMockIterator res_iter;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  ObVersionRange trans_version_range;
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
  scan_merge.reset();
}

TEST_F(TestSkipFetchByBaseVersion, test_rowkey_cross_mb)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    10    10    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    20    20    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    30    30    INSERT    NORMAL      CLF\n";

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
      "1        -10      MIN         11    11     INSERT    NORMAL        SCF\n"
      "1        -10      DI_VERSION  11    11     INSERT    NORMAL        C\n"
      "1        -10      0           10    10     DELETE    NORMAL        CL\n"
      "2        -20      MIN         22    22     INSERT    NORMAL        SCF\n"
      "2        -20      DI_VERSION  22    22     INSERT    NORMAL        C\n"
      "2        -20      0           21    21     DELETE    NORMAL        C\n";
  micro_data2[1] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -10      DI_VERSION  21    21     INSERT    NORMAL        C\n"
      "2        -10      0           20    20     DELETE    NORMAL        CL\n";
  micro_data2[2] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "3        -60      MIN         31    31     INSERT    NORMAL        SCF\n"
      "3        -60      DI_VERSION  31    31     INSERT    NORMAL        C\n"
      "3        -60      0           30    30     DELETE    NORMAL        CL\n";

  snapshot_version = 100;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(100);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 3);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "3        31      31    INSERT    NORMAL\n"
      "1        10      10    INSERT    NORMAL\n"
      "2        20      20    INSERT    NORMAL\n";

  ObMockIterator res_iter;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  ObVersionRange trans_version_range;
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
  ASSERT_EQ(3, total_count);

  handle1.reset();
  handle2.reset();
  scan_merge.reset();
}

TEST_F(TestSkipFetchByBaseVersion, test_multi_sstables_cross_version)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  // major sstable
  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    10      10    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    20      20    INSERT    NORMAL      CLF\n"
      "3        -50      DI_VERSION    30      30    INSERT    NORMAL      CLF\n";
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
      "1        -10      MIN        11     10     INSERT    NORMAL        SCF\n"
      "1        -10      DI_VERSION 11     10     INSERT    NORMAL        C\n"
      "1        -10      0          10     10     DELETE    NORMAL        CL\n"
      "2        -10      MIN        21     20     INSERT    NORMAL        SCF\n"
      "2        -10      DI_VERSION 21     20     INSERT    NORMAL        C\n"
      "2        -10      0          20     20     DELETE    NORMAL        CL\n"
      "3        -10      MIN        31     30     INSERT    NORMAL        SCF\n"
      "3        -10      DI_VERSION 31     30     INSERT    NORMAL        C\n"
      "3        -10      0          30     30     DELETE    NORMAL        CL\n";

  snapshot_version = 10;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(10);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  // minor sstable 2
  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -10      MIN        11     10     INSERT    NORMAL        SCF\n"
      "1        -10      DI_VERSION 11     10     INSERT    NORMAL        C\n"
      "1        -10      0          10     10     DELETE    NORMAL        CL\n"
      "2        -60      MIN        22     20     INSERT    NORMAL        SCF\n"
      "2        -60      DI_VERSION 22     20     INSERT    NORMAL        C\n"
      "2        -60      0          21     20     DELETE    NORMAL        CL\n"
      "3        -60      MIN        32     30     INSERT    NORMAL        SCF\n"
      "3        -60      DI_VERSION 32     30     INSERT    NORMAL        C\n"
      "3        -60      0          31     30     DELETE    NORMAL        CL\n";

  snapshot_version = 60;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(60);
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
      "1        -60      MIN        12     10     INSERT    NORMAL        SCF\n"
      "1        -60      DI_VERSION 12     10     INSERT    NORMAL        C\n"
      "1        -60      0          11     10     DELETE    NORMAL        CL\n"
      "2        -60      MIN        22     20     INSERT    NORMAL        SCF\n"
      "2        -60      DI_VERSION 22     20     INSERT    NORMAL        C\n"
      "2        -60      0          21     20     DELETE    NORMAL        CL\n"
      "3        -70      MIN        33     30     INSERT    NORMAL        SCF\n"
      "3        -70      DI_VERSION 33     30     INSERT    NORMAL        C\n"
      "3        -70      0          32     30     DELETE    NORMAL        CL\n";

  snapshot_version = 100;
  scn_range.start_scn_.convert_for_tx(60);
  scn_range.end_scn_.convert_for_tx(100);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data4, 1);
  prepare_data_end(handle4);
  table_store_iter.add_table(handle4.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable4");

  ObDatumRange range;
  range.set_whole_range();
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 50;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = 100;
  prepare_scan_param(trans_version_range, table_store_iter);

  ObMultipleScanMerge scan_merge;
  ASSERT_EQ(OB_SUCCESS, scan_merge.init(access_param_, context_, get_table_param_));
  ASSERT_EQ(OB_SUCCESS, scan_merge.open(range));
  scan_merge.disable_padding();
  scan_merge.disable_fill_virtual_column();

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "1         12      10    INSERT    NORMAL\n"
      "2         22      20    INSERT    NORMAL\n"
      "3         33      30    INSERT    NORMAL\n";

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
  handle4.reset();
  scan_merge.reset();
}

TEST_F(TestSkipFetchByBaseVersion, test_uncommitted_row)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] =
      "bigint   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -50      DI_VERSION    10    10    INSERT    NORMAL      CLF\n"
      "2        -50      DI_VERSION    20    20    INSERT    NORMAL      CLF\n";

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
      "1        MIN      -100        11    10     DELETE    NORMAL        UCF\n";
  micro_data2[1] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        -10      DI_VERSION  11    10     INSERT    NORMAL        C\n"
      "1        -10      0           10    10     DELETE    NORMAL        CL\n"
      "2        MIN      -99         21    20     DELETE    NORMAL        UCF\n";
  micro_data2[2] =
      "bigint   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        -10      DI_VERSION  21    20     INSERT    NORMAL        C\n"
      "2        -10      0           20    20     DELETE    NORMAL        CL\n"
      "3        MIN      -98         30    30     INSERT    NORMAL        UCFL\n";

  snapshot_version = 100;
  scn_range.start_scn_.convert_for_tx(50);
  scn_range.end_scn_.convert_for_tx(100);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 3);
  prepare_data_end(handle2);
  table_store_iter.add_table(handle2.get_table());
  STORAGE_LOG(INFO, "finish prepare sstable2");

  const char *result1 =
      "bigint   bigint bigint  flag     flag_type\n"
      "3        30      30    INSERT    NORMAL\n";

  ObMockIterator res_iter;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  ObVersionRange trans_version_range;
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
  ASSERT_EQ(1, total_count);

  handle1.reset();
  handle2.reset();
  scan_merge.reset();
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_skip_fetch_by_base_version.log*");
  OB_LOGGER.set_file_name("test_skip_fetch_by_base_version.log");
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
