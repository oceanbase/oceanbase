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

namespace oceanbase {
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

namespace storage {

ObSEArray<ObTxData, 8> TX_DATA_ARR;

int ObTxTable::insert(ObTxData *&tx_data)
{
  int ret = OB_SUCCESS;
  ret = TX_DATA_ARR.push_back(*tx_data);
  return ret;
}

int ObTxTable::check_with_tx_data(ObReadTxDataArg &read_tx_data_arg,
                                  ObITxDataCheckFunctor &fn)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < TX_DATA_ARR.count(); i++) {
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
        STORAGE_LOG(ERROR, "check with tx data failed", KR(ret),
                    K(read_tx_data_arg), K(TX_DATA_ARR.at(i)));
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

class ObMockWhiteFilterExecutor : public ObWhiteFilterExecutor
{
public:
  ObMockWhiteFilterExecutor(common::ObIAllocator &alloc,
                            ObPushdownWhiteFilterNode &filter,
                            ObPushdownOperator &op)
      : ObWhiteFilterExecutor(alloc, filter, op) {}

  virtual int init_evaluated_datums(bool &is_valid) override {
    UNUSED(is_valid);
    return OB_SUCCESS;
  };
};

class TestScanBasic : public TestMergeBasic
{
public:
  static const int64_t MAX_PARALLEL_DEGREE = 10;
  TestScanBasic(const char *test_name);
  virtual ~TestScanBasic() {}

  void SetUp();
  void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
  static void reset_tablet_table_store();
  static void get_tablet_table_store(ObTabletTableStore *&store);
  void prepare_data_end_with_param(ObTableHandleV2 &handle,
                                   ObTabletCreateSSTableParam &param,
                                   const int64_t upper_trans_version);
  void prepare_create_basic_sst_param(ObTabletCreateSSTableParam &param,
                                      const ObScnRange &scn_range,
                                      const ObITable::TableType &table_type);
  void prepare_create_row_sst_param(ObTabletCreateSSTableParam &param);
  void prepare_create_column_sst_param(ObTabletCreateSSTableParam &param);
  void prepare_create_inc_sst_param(ObTabletCreateSSTableParam &param,
                                    const std::vector<compaction::ObUncommitTxDesc> &uncommit_info);
  void prepare_get_param(const ObVersionRange &version_range,
                          const ObTableStoreIterator &table_store_iter);
  void prepare_scan_param(const ObVersionRange &version_range,
                          const ObTableStoreIterator &table_store_iter,
                          const bool is_delete_insert);
  void prepare_output_expr(const ObIArray<int32_t> &projector,
                           const ObIArray<ObColDesc> &cols_desc);
  int create_pushdown_filter(const bool is_white, const int64_t col_id,
                             const ObDatum &datum,
                             const ObWhiteFilterOperatorType &op_type,
                             const ObITableReadInfo &read_info,
                             ObPushdownFilterExecutor *&pushdown_filter);
  void prepare_txn(ObStoreCtx *store_ctx, const int64_t prepare_version);

  void fake_freeze_info();
  void get_tx_table_guard(ObTxTableGuard &tx_table_guard);
  void mock_tablet_table_store(ObTableStoreIterator &table_store_iter);
  void refresh_iter(ObMultipleMerge &merge);
  int refresh_table(ObMultipleMerge &merge,
                    ObTableStoreIterator &table_store_iter);
  void convert_to_co_sstable(ObTableHandleV2 &row_store,
                             ObTableHandleV2 &co_store);
  void generate_range(const int64_t start,
                      const int64_t end,
                      ObDatumRange &range);
  void generate_rowkey(const int64_t idx,
                       ObDatumRowkey &rowkey);

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
  ObFixedArray<share::schema::ObColumnParam *, ObIAllocator> cols_param_;
  sql::ExprFixedArray output_exprs_;
  sql::ObExecContext exec_ctx_;
  sql::ObEvalCtx eval_ctx_;
  sql::ObPushdownExprSpec expr_spec_;
  sql::ObPushdownOperator op_;
  void *datum_buf_;
  int64_t datum_buf_offset_;
  bool memstore_retired_;
  ObRowGenerate row_generate_;
  ObDatumRow start_row_;
  ObDatumRow end_row_;
};

ObLSTxCtxMgr TestScanBasic::ls_tx_ctx_mgr_;

void TestScanBasic::SetUpTestCase()
{
  ObMultiVersionSSTableTest::SetUpTestCase();
  // mock sequence no
  ObClockGenerator::init();

  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_EQ(OB_SUCCESS,
            ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  MTL(ObTenantTabletScheduler *)->resume_major_merge();

  // create tablet
  share::schema::ObTableSchema table_schema;
  uint64_t table_id = 12345;
  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, table_id));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(
                            ls_handle, tablet_id, table_schema, allocator_));

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  ls_handle.get_ls()->get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());
}

void TestScanBasic::TearDownTestCase()
{
  reset_tablet_table_store();
  ObMultiVersionSSTableTest::TearDownTestCase();
  ls_tx_ctx_mgr_.reset();
  ls_tx_ctx_mgr_.ls_tx_ctx_map_.reset();
  // reset sequence no
  ObClockGenerator::destroy();
}

TestScanBasic::TestScanBasic(const char *test_name)
    : TestMergeBasic(test_name), exec_ctx_(query_allocator_),
      eval_ctx_(exec_ctx_), expr_spec_(query_allocator_),
      op_(eval_ctx_, expr_spec_), memstore_retired_(false) {}

void TestScanBasic::SetUp()
{
  ObMultiVersionSSTableTest::SetUp();
}

void TestScanBasic::fake_freeze_info()
{
  share::ObFreezeInfoList &info_list =
      MTL(ObTenantFreezeInfoMgr *)->freeze_info_mgr_.freeze_info_;
  info_list.reset();

  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, info_list.frozen_statuses_.push_back(
                            share::ObFreezeInfo(frozen_val, 1, 0)));
  frozen_val.val_ = 100;
  ASSERT_EQ(OB_SUCCESS, info_list.frozen_statuses_.push_back(
                            share::ObFreezeInfo(frozen_val, 1, 0)));
  frozen_val.val_ = 200;
  ASSERT_EQ(OB_SUCCESS, info_list.frozen_statuses_.push_back(
                            share::ObFreezeInfo(frozen_val, 1, 0)));
  frozen_val.val_ = 400;
  ASSERT_EQ(OB_SUCCESS, info_list.frozen_statuses_.push_back(
                            share::ObFreezeInfo(frozen_val, 1, 0)));

  info_list.latest_snapshot_gc_scn_.val_ = 500;
}

void TestScanBasic::TearDown()
{
  ObMultiVersionSSTableTest::TearDown();
  TRANS_LOG(INFO, "teardown success");
}

void TestScanBasic::prepare_txn(ObStoreCtx *store_ctx,
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

void TestScanBasic::prepare_data_end_with_param(
    ObTableHandleV2 &handle,
    ObTabletCreateSSTableParam &param,
    const int64_t upper_trans_version)
{
  param.table_key_ = table_key_;
  if (table_key_.table_type_ == ObITable::COLUMN_ORIENTED_SSTABLE
      || table_key_.table_type_ == ObITable::INC_COLUMN_ORIENTED_SSTABLE) {
    OK(ObTabletCreateDeleteHelper::create_sstable<ObCOSSTableV2>(
        param, allocator_, handle));
  } else {
    OK(ObTabletCreateDeleteHelper::create_sstable(param, allocator_, handle));
  }
  if (table_key_.table_type_ == ObITable::MINI_SSTABLE
      || table_key_.table_type_ == ObITable::MINOR_SSTABLE
      || table_key_.table_type_ == ObITable::INC_MAJOR_SSTABLE
      || table_key_.table_type_ == ObITable::INC_COLUMN_ORIENTED_SSTABLE) {
    static_cast<ObSSTable *>(handle.get_table())->set_upper_trans_version(allocator_, upper_trans_version);
  }
}

void TestScanBasic::prepare_create_basic_sst_param(
    ObTabletCreateSSTableParam &param,
    const ObScnRange &scn_range,
    const ObITable::TableType &table_type)
{
  table_key_.scn_range_ = scn_range;
  table_key_.table_type_ = table_type;
  ASSERT_EQ(OB_SUCCESS, macro_writer_.close());
  ObSSTableMergeRes res;
  const int64_t column_cnt =
      table_schema_.get_column_count() +
      ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  ASSERT_EQ(OB_SUCCESS, root_index_builder_->close(res));
  const int64_t upper_trans_version = res.contain_uncommitted_row_ ?
      INT64_MAX : res.max_merged_trans_version_;
  param.set_init_value_for_column_store_();
  ASSERT_EQ(OB_SUCCESS, param.data_block_ids_.assign(res.data_block_ids_));
  ASSERT_EQ(OB_SUCCESS, param.other_block_ids_.assign(res.other_block_ids_));
  param.schema_version_ = SCHEMA_VERSION;
  param.create_snapshot_version_ = 0;
  param.progressive_merge_round_ = table_schema_.get_progressive_merge_round();
  param.progressive_merge_step_ = 0;
  param.table_mode_ = table_schema_.get_table_mode_struct();
  param.index_type_ = table_schema_.get_index_type();
  param.latest_row_store_type_ = table_schema_.get_row_store_type();
  param.rowkey_column_cnt_ =
      table_schema_.get_rowkey_column_num() +
      ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();

  ObSSTableMergeRes::fill_addr_and_data(res.root_desc_, param.root_block_addr_,
                                        param.root_block_data_);
  ObSSTableMergeRes::fill_addr_and_data(res.data_root_desc_,
                                        param.data_block_macro_meta_addr_,
                                        param.data_block_macro_meta_);
  param.is_meta_root_ = res.data_root_desc_.is_meta_root_;
  param.root_row_store_type_ = res.root_row_store_type_;
  param.data_index_tree_height_ = res.root_desc_.height_;
  param.index_blocks_cnt_ = res.index_blocks_cnt_;
  param.data_blocks_cnt_ = res.data_blocks_cnt_;
  param.micro_block_cnt_ = res.micro_block_cnt_;
  param.use_old_macro_block_count_ = 0;
  param.column_cnt_ = column_cnt;
  param.data_checksum_ = 0;
  param.occupy_size_ = 0;
  param.original_size_ = 0;
  param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param.encrypt_id_ = 0;
  param.master_key_id_ = 0;
  param.nested_size_ = res.nested_size_;
  param.nested_offset_ = res.nested_offset_;
  param.ddl_scn_.set_min();
  param.table_backup_flag_.reset();
  param.table_shared_flag_.reset();
  param.filled_tx_scn_ = table_key_.get_end_scn();
  param.tx_data_recycle_scn_.set_min();
  param.sstable_logic_seq_ = 0;
  param.row_count_ = 0;
  param.recycle_version_ = 0;
  param.root_macro_seq_ = 0;
  param.rec_scn_.set_min();
  param.max_merged_trans_version_ = res.max_merged_trans_version_;
  param.contain_uncommitted_row_ = res.contain_uncommitted_row_;
  param.upper_trans_version_ = upper_trans_version;
}

void TestScanBasic::prepare_create_row_sst_param(
    ObTabletCreateSSTableParam &param)
{
  if (table_key_.table_type_ == ObITable::MAJOR_SSTABLE) {
    table_key_.version_range_.snapshot_version_ = table_key_.scn_range_.end_scn_.val_;
  }
  OK(ObSSTableMergeRes::fill_column_checksum_for_empty_major(param.column_cnt_, param.column_checksums_));
}

void TestScanBasic::prepare_create_column_sst_param(
    ObTabletCreateSSTableParam &param)
{
  table_key_.version_range_.snapshot_version_ = table_key_.scn_range_.end_scn_.val_;
  param.table_key_.column_group_idx_ = 0;
}

void TestScanBasic::prepare_create_inc_sst_param(
    ObTabletCreateSSTableParam &param,
    const std::vector<compaction::ObUncommitTxDesc> &uncommit_info)
{
  param.uncommit_tx_info_.reuse();
  for (int64_t i = 0; i < uncommit_info.size(); ++i) {
    param.uncommit_tx_info_.push_back(uncommit_info.at(i));
  }
}

void TestScanBasic::prepare_get_param(
  const ObVersionRange &version_range,
  const ObTableStoreIterator &table_store_iter)
{
  context_.reset();
  access_param_.reset();
  get_table_param_.reset();
  read_info_.reset();
  output_cols_project_.reset();
  cols_param_.reset();
  datum_buf_ = nullptr;
  datum_buf_offset_ = 0;
  query_allocator_.reset();

  ObTabletHandle tablet_handle;
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_EQ(OB_SUCCESS,
            ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS,
            ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));

  get_table_param_.frozen_version_ = -1;
  get_table_param_.refreshed_merge_ = nullptr;
  get_table_param_.need_split_dst_table_ = false;
  get_table_param_.tablet_iter_.tablet_handle_.assign(tablet_handle);
  get_table_param_.tablet_iter_.table_store_iter_.assign(table_store_iter);
  get_table_param_.tablet_iter_.transfer_src_handle_ = nullptr;
  get_table_param_.tablet_iter_.split_extra_tablet_handles_.reset();

  int64_t schema_column_count = full_read_info_.get_schema_column_count();
  int64_t schema_rowkey_count = full_read_info_.get_schema_rowkey_count();
  int64_t request_count = full_read_info_.get_request_count();
  full_read_info_.trans_col_index_ = schema_rowkey_count;
  output_cols_project_.set_allocator(&query_allocator_);
  output_cols_project_.init(request_count);
  for (int64_t i = 0; i < request_count; i++) {
    output_cols_project_.push_back(i);
  }

  access_param_.iter_param_.read_info_ = &full_read_info_;
  access_param_.iter_param_.rowkey_read_info_ = &full_read_info_;
  access_param_.iter_param_.table_id_ = table_id_;
  access_param_.iter_param_.tablet_id_ = tablet_id_;
  access_param_.iter_param_.is_same_schema_column_ = true;
  access_param_.iter_param_.has_virtual_columns_ = false;
  access_param_.iter_param_.vectorized_enabled_ = false;
  access_param_.iter_param_.has_lob_column_out_ = false;
  access_param_.iter_param_.pd_storage_flag_.pd_blockscan_ = false;
  access_param_.iter_param_.pd_storage_flag_.pd_filter_ = false;
  access_param_.iter_param_.out_cols_project_ = &output_cols_project_;
  access_param_.padding_cols_ = nullptr;
  access_param_.aggregate_exprs_ = nullptr;
  access_param_.op_filters_ = nullptr;
  access_param_.output_sel_mask_ = nullptr;
  access_param_.is_inited_ = true;
  share::SCN snapshot_scn;
  snapshot_scn.convert_for_tx(version_range.snapshot_version_);
  ASSERT_EQ(OB_SUCCESS, store_ctx_.init_for_read(ls_id, tablet_id,
                                                 INT64_MAX, // query_expire_ts
                                                 -1,        // lock_timeout_us
                                                 snapshot_scn));

  ObQueryFlag query_flag(ObQueryFlag::Forward,
                         false,                  // daily_merge
                         false,                  // optimize
                         false,                  // sys scan
                         false,                  // full_row
                         false,                  // index_back
                         false,                  // query_stat
                         ObQueryFlag::MysqlMode, // sql_mode
                         true                    // read_latest
  );
  query_flag.set_not_use_row_cache();
  query_flag.set_not_use_block_cache();
  // query_flag.multi_version_minor_merge_ = true;
  ASSERT_EQ(OB_SUCCESS, context_.init(query_flag, store_ctx_, query_allocator_,
                                      query_allocator_, version_range));
  context_.ls_id_ = ls_id_;
  context_.limit_param_ = nullptr;
  context_.is_inited_ = true;
}

void TestScanBasic::prepare_scan_param(
    const ObVersionRange &version_range,
    const ObTableStoreIterator &table_store_iter,
    const bool is_delete_insert)
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
  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_EQ(OB_SUCCESS,
            ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS,
            ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));

  get_table_param_.frozen_version_ = -1;
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
  const common::ObIArray<ObColDesc> &cols_desc =
      full_read_info_.get_columns_desc();
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
    ObColumnParam *col_param =
        new (col_param_buf) ObColumnParam(query_allocator_);
    col_param->set_meta_type(tmp_col_descs.at(i).col_type_);
    col_param->set_nullable_for_write(true);
    col_param->set_column_id(common::OB_APP_MIN_COLUMN_ID + i);
    cols_param_.push_back(col_param);
    tmp_cg_idxs.push_back(i + 1);
  }

  ASSERT_EQ(OB_SUCCESS, read_info_.init(query_allocator_,
                                        full_read_info_.get_schema_column_count(),
                                        full_read_info_.get_schema_rowkey_count(),
                                        lib::is_oracle_mode(),
                                        tmp_col_descs,
                                        nullptr /*storage_cols_index*/,
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
  access_param_.iter_param_.is_delete_insert_ = is_delete_insert;

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
  access_param_.row2exprs_projector_ =
      new (buf) ObRow2ExprsProjector(query_allocator_);
  // TODO: construct pushdown filter
  ASSERT_EQ(OB_SUCCESS,
            access_param_.iter_param_.op_->init_pushdown_storage_filter());
  access_param_.is_inited_ = true;
  share::SCN snapshot_scn;
  snapshot_scn.convert_for_tx(version_range.snapshot_version_);
  ASSERT_EQ(OB_SUCCESS, store_ctx_.init_for_read(ls_id, tablet_id,
                                                 INT64_MAX, // query_expire_ts
                                                 -1,        // lock_timeout_us
                                                 snapshot_scn));

  ObQueryFlag query_flag(is_delete_insert ? ObQueryFlag::NoOrder : ObQueryFlag::Forward,
                         false,                  // daily_merge
                         false,                  // optimize
                         false,                  // sys scan
                         false,                  // full_row
                         false,                  // index_back
                         false,                  // query_stat
                         ObQueryFlag::MysqlMode, // sql_mode
                         true                    // read_latest
  );
  query_flag.set_not_use_row_cache();
  query_flag.set_not_use_block_cache();
  // query_flag.multi_version_minor_merge_ = true;
  ASSERT_EQ(OB_SUCCESS, context_.init(query_flag, store_ctx_, query_allocator_,
                                      query_allocator_, version_range));
  context_.ls_id_ = ls_id_;
  context_.limit_param_ = nullptr;
  context_.is_inited_ = true;
}

void TestScanBasic::prepare_output_expr(
    const ObIArray<int32_t> &projector, const ObIArray<ObColDesc> &cols_desc)
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
    sql::ObDatum *datums = new ((char *)datum_buf_ + datum_buf_offset_)
        sql::ObDatum[DATUM_ARRAY_CNT];
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

int TestScanBasic::create_pushdown_filter(
    const bool is_white, const int64_t col_id, const ObDatum &datum,
    const ObWhiteFilterOperatorType &op_type, const ObITableReadInfo &read_info,
    ObPushdownFilterExecutor *&pushdown_filter)
{
  int ret = OB_SUCCESS;
  ObMockWhiteFilterExecutor *filter = nullptr;
  ObIAllocator *allocator_ptr = &query_allocator_;
  ExprFixedArray *column_exprs = nullptr;
  if (!is_white) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "Not support to create not white filter", K(ret));
  } else {
    ObPushdownWhiteFilterNode *white_node =
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
    } else if (OB_FAIL(filter->cg_col_exprs_.push_back(
                   access_param_.output_exprs_->at(
                       col_id - common::OB_APP_MIN_COLUMN_ID)))) {
      STORAGE_LOG(WARN, "Fail to push back col expr", K(ret));
    } else {
      filter->cmp_func_ = get_datum_cmp_func(
          cols_desc.at(col_id - common::OB_APP_MIN_COLUMN_ID).col_type_,
          cols_desc.at(col_id - common::OB_APP_MIN_COLUMN_ID).col_type_);
      STORAGE_LOG(INFO, "finish create pushdown filter");
    }
  }
  return ret;
}

void TestScanBasic::get_tx_table_guard(ObTxTableGuard &tx_table_guard)
{
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_EQ(OB_SUCCESS,
            ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tx_table_guard(tx_table_guard));
}

void TestScanBasic::mock_tablet_table_store(ObTableStoreIterator &table_store_iter)
{
  int ret = OB_SUCCESS;
  table_store_iter.resume();
  ObTabletTableStore *store = nullptr;
  TestScanBasic::get_tablet_table_store(store);
  ObSSTableArray &major_tables = store->major_tables_;
  ObSSTableArray &inc_major_tables = store->inc_major_tables_;
  ObSSTableArray &minor_tables = store->minor_tables_;
  ObSEArray<ObITable *, OB_DEFAULT_SE_ARRAY_COUNT> new_major_tables;
  ObSEArray<ObITable *, OB_DEFAULT_SE_ARRAY_COUNT> new_inc_major_tables;
  ObSEArray<ObITable *, OB_DEFAULT_SE_ARRAY_COUNT> new_minor_tables;
  ObTableHandleV2 table_hdl;
  for (int64_t i = 0; i < table_store_iter.count(); ++i) {
    OK(table_store_iter.get_next(table_hdl));
    if (ObITable::MAJOR_SSTABLE == table_hdl.get_table()->get_table_type()
        || ObITable::COLUMN_ORIENTED_SSTABLE == table_hdl.get_table()->get_table_type()) {
      OK(new_major_tables.push_back(table_hdl.get_table()));
    } else if (ObITable::INC_MAJOR_SSTABLE == table_hdl.get_table()->get_table_type()
               || ObITable::INC_COLUMN_ORIENTED_SSTABLE == table_hdl.get_table()->get_table_type()) {
      OK(new_inc_major_tables.push_back(table_hdl.get_table()));
    } else if (ObITable::MINI_SSTABLE == table_hdl.get_table()->get_table_type()
               || ObITable::MINOR_SSTABLE == table_hdl.get_table()->get_table_type()) {
      OK(new_minor_tables.push_back(table_hdl.get_table()));
    } else {
      LOG_WARN("get unexpected table type", K(table_hdl.get_table()->get_table_type()));
      FAIL() << "get unexpected table type";
    }
  }
  major_tables.reset();
  inc_major_tables.reset();
  minor_tables.reset();
  OK(major_tables.init(allocator_, new_major_tables));
  OK(inc_major_tables.init(allocator_, new_inc_major_tables));
  OK(minor_tables.init(allocator_, new_minor_tables));
  table_store_iter.resume();
  LOG_INFO("mock tablet table store", K(major_tables), K(inc_major_tables), K(minor_tables));
}

void TestScanBasic::refresh_iter(ObMultipleMerge &merge)
{
  OK(merge.refresh_tablet_iter());
  OK(merge.prepare_read_tables(false/*refresh*/));
  OK(merge.switch_param(access_param_, context_, get_table_param_));
  LOG_INFO("refresh iter", K(merge.get_table_param_->tablet_iter_.table_store_iter_));
}

void TestScanBasic::reset_tablet_table_store()
{
  int ret = OB_SUCCESS;
  ObTabletTableStore *store = nullptr;
  get_tablet_table_store(store);
  store->major_tables_.reset();
  store->inc_major_tables_.reset();
  store->minor_tables_.reset();
}

void TestScanBasic::get_tablet_table_store(ObTabletTableStore *&store)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService *);
  const ObTabletTableStore *const_store = nullptr;
  const ObTablet *tablet = nullptr;
  OK(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  OK(ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
  tablet = tablet_handle.get_obj();
  if (tablet->table_store_addr_.is_memory_object()) {
    const_store = tablet->table_store_addr_.get_ptr();
  } else {
    ObStorageMetaHandle table_store_handle;
    ObStorageMetaKey meta_key(MTL_ID(), tablet->table_store_addr_.addr_);
    const ObStorageMetaValue *value = nullptr;
    OK(OB_STORE_CACHE.get_storage_meta_cache().get_meta(
        ObStorageMetaValue::MetaType::TABLE_STORE, meta_key, table_store_handle, tablet));
    OK(table_store_handle.get_value(value));
    OK(value->get_table_store(const_store));
  }
  store = const_cast<ObTabletTableStore *>(const_store);
}

int TestScanBasic::refresh_table(ObMultipleMerge &merge,
                                 ObTableStoreIterator &table_store_iter)
{
  int ret = OB_SUCCESS;
  memstore_retired_ = true;
  table_store_iter.memstore_retired_ = &memstore_retired_;
  get_table_param_.frozen_version_ = -1;
  if (OB_FAIL(merge.refresh_table_on_demand())) {
    STORAGE_LOG(WARN, "fail to refresh table on demand", K(ret));
  }
  return ret;
}

int create_cg_sstables(ObCOTabletMergeCtx &ctx, const int64_t start_cg_idx, const int64_t end_cg_idx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = start_cg_idx; OB_SUCC(ret) && i < end_cg_idx; ++i) {
    if (OB_FAIL(ctx.create_cg_sstable(i))) {
      LOG_WARN("failed to create cg sstable", K(ret), K(i));
    }
  }
  return ret;
}

void TestScanBasic::convert_to_co_sstable(ObTableHandleV2 &row_store,
                                          ObTableHandleV2 &co_store)
{
  int ret = OB_SUCCESS;
  ObCOMergeDagParam param;
  ObCOMergeDagNet dag_net;
  param.compat_mode_ = lib::Worker::CompatMode::MYSQL;
  int64_t column_cnt = table_schema_.get_column_count();
  int64_t cg_cnt = column_cnt + 1;
  ObCOTabletMergeCtx merge_context(dag_net, param, allocator_);
  unittest::TestSchemaPrepare::add_all_and_each_column_group(allocator_, table_schema_);
  merge_context.static_param_.tables_handle_.add_table(row_store);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = row_store.get_table()->get_snapshot_version();
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  //prepare merge_ctx
  TestMergeBasic::prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
  merge_context.static_param_.co_static_param_.co_major_merge_type_ = ObCOMajorMergePolicy::USE_RS_BUILD_SCHEMA_MATCH_MERGE;
  merge_context.static_param_.data_version_ = DATA_VERSION_4_3_0_0;
  merge_context.static_param_.dag_param_.merge_version_ = trans_version_range.snapshot_version_;
  int32_t base_cg_idx = -1;
  OK(merge_context.cal_merge_param());
  OK(merge_context.init_parallel_merge_ctx());
  OK(merge_context.static_param_.init_static_info(merge_context.tablet_handle_));
  OK(merge_context.init_static_desc());
  OK(merge_context.init_read_info());
  merge_context.array_count_ = cg_cnt;
  OK(merge_context.init_tablet_merge_info());
  OK(merge_context.prepare_index_builder(0, cg_cnt));
  OK(merge_context.get_schema()->get_base_rowkey_column_group_index(base_cg_idx));
  merge_context.base_rowkey_cg_idx_ = base_cg_idx;
  ObCOMergeDagParam *dag_param = static_cast<ObCOMergeDagParam *>(&merge_context.static_param_.dag_param_);
  dag_param->compat_mode_ = lib::Worker::CompatMode::MYSQL;
  ObCOMergeLogReplayer replayer(local_arena_, merge_context.static_param_, 0, cg_cnt, true);
  OK(replayer.init(merge_context, 0));
  OK(replayer.replay_merge_log());
  OK(create_cg_sstables(merge_context, 0, cg_cnt));
  ASSERT_FALSE(cg_cnt != merge_context.merged_cg_tables_handle_.get_count());
  common::ObArray<ObITable *> cg_tables;
  ObCOSSTableV2 *co_sstable = nullptr;
  storage::ObTablesHandleArray &merged_cg_tables_handle = merge_context.merged_cg_tables_handle_;
  for (int64_t i = 0; i < cg_cnt; i++) {
    ObSSTable *merged_sstable = static_cast<ObSSTable *>(merged_cg_tables_handle.get_table(i));
    assert(merged_sstable);
    if (!merged_sstable->is_co_sstable()) {
      cg_tables.push_back(merged_sstable);
    } else {
      co_sstable = static_cast<ObCOSSTableV2 *>(merged_sstable);
    }
  }
  assert(co_sstable);
  OK(co_sstable->fill_cg_sstables(cg_tables));
  co_store.set_sstable(co_sstable, &allocator_);
}

void TestScanBasic::generate_range(const int64_t start,
                                   const int64_t end,
                                   ObDatumRange &range)
{
  const int64_t column_cnt = full_read_info_.get_schema_column_count();
  if (!start_row_.is_valid()) {
    OK(start_row_.init(allocator_, column_cnt));
  }
  if (!end_row_.is_valid()) {
    OK(end_row_.init(allocator_, column_cnt));
  }

  ObDatumRowkey tmp_rowkey;
  const int64_t rowkey_column_cnt = full_read_info_.get_schema_rowkey_count();
  row_generate_.reset();
  OK(row_generate_.init(table_schema_, &allocator_));
  OK(row_generate_.get_next_row(start, start_row_));
  tmp_rowkey.assign(start_row_.storage_datums_, rowkey_column_cnt);
  OK(tmp_rowkey.deep_copy(range.start_key_, allocator_));
  OK(row_generate_.get_next_row(end, end_row_));
  tmp_rowkey.assign(end_row_.storage_datums_, rowkey_column_cnt);
  OK(tmp_rowkey.deep_copy(range.end_key_, allocator_));
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
}

void TestScanBasic::generate_rowkey(const int64_t idx, ObDatumRowkey &rowkey)
{
  const int64_t column_cnt = full_read_info_.get_request_count();
  const int64_t rowkey_column_cnt = full_read_info_.get_schema_rowkey_count();
  if (!start_row_.is_valid()) {
    OK(start_row_.init(allocator_, column_cnt));
  }
  row_generate_.reset();
  OK(row_generate_.init(table_schema_, &allocator_));
  OK(row_generate_.get_next_row(idx, start_row_));
  ObDatumRowkey tmp_rowkey;
  tmp_rowkey.assign(start_row_.storage_datums_, rowkey_column_cnt);
  tmp_rowkey.deep_copy(rowkey, allocator_);
}
} // namespace storage
} // namespace oceanbase
