/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *      http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OB_TEST_MERGE_BASIC_H_
#define OB_TEST_MERGE_BASIC_H_
#define private public
#define protected public
#define UNITTEST
#include "storage/blocksstable/ob_multi_version_sstable_test.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/test_tablet_helper.h"
#include "storage/compaction/filter/ob_mds_info_compaction_filter.h"
#include "storage/compaction_ttl/ob_ttl_filter.h"
#include "storage/compaction_ttl/ob_ttl_filter_info_array.h"
#include "unittest/storage/ob_ttl_filter_info_helper.h"
#include "storage/compaction/filter/ob_rowscn_filter.h"
#include "storage/column_store/ob_co_merge_ctx.h"
#include "storage/column_store/ob_column_oriented_merger.h"
#include "storage/column_store/ob_co_merge_dag.h"

namespace oceanbase
{
namespace storage
{
class TestMergeBasic : public ObMultiVersionSSTableTest
{
public:
  enum class ObCOMergeTestType : uint8_t
  {
    NORMAL = 0, // build merge log then replay
    USE_ROW_TMP_FILE,
    USE_COLUMN_TMP_FILE,
    NORMAL_WITH_BASE_REPLAY, // build merge log then replay
    USE_ROW_TMP_FILE_WITH_BASE_REPLAY,
    USE_COLUMN_TMP_FILE_WITH_BASE_REPLAY,
    MAX_TSET_TYPE
  };
  static bool need_replay_base(const ObCOMergeTestType &type)
  {
    return ObCOMergeTestType::NORMAL_WITH_BASE_REPLAY <= type && ObCOMergeTestType::MAX_TSET_TYPE > type;
  }

  static bool is_normal_test_type(const ObCOMergeTestType &type)
  {
    return ObCOMergeTestType::NORMAL == type || ObCOMergeTestType::NORMAL_WITH_BASE_REPLAY == type;
  }

  static bool is_column_tmp_file_test_type(const ObCOMergeTestType &type)
  {
    return ObCOMergeTestType::USE_COLUMN_TMP_FILE == type || ObCOMergeTestType::USE_COLUMN_TMP_FILE_WITH_BASE_REPLAY == type;
  }

  static bool is_row_tmp_file_test_type(const ObCOMergeTestType &type)
  {
    return ObCOMergeTestType::USE_ROW_TMP_FILE == type || ObCOMergeTestType::USE_ROW_TMP_FILE_WITH_BASE_REPLAY == type;
  }

  TestMergeBasic(const char *test_name)
  : ObMultiVersionSSTableTest(test_name)
  {}
  void prepare_merge_context(
    const ObMergeType &merge_type,
    const bool is_full_merge,
    const ObVersionRange &trans_version_range,
    compaction::ObBasicTabletMergeCtx &merge_context);

  template <typename T>
  void prepare_merge_context(
    const ObMergeType &merge_type,
    const bool is_full_merge,
    const ObVersionRange &trans_version_range,
    ObTabletMergeDag *merge_dag,
    T &merge_context,
    const bool is_delete_insert_merge = false);
  void prepare_co_major_merge_context(
      const ObMergeType &merge_type,
      const bool is_full_merge,
      const ObVersionRange &trans_version_range,
      ObTabletMergeDag *merge_dag,
      ObCOTabletMergeCtx &merge_context,
      const bool is_delete_insert_merge = false);
  void get_tx_table_guard(ObTxTableGuard &tx_table_guard);
  void insert_tx_data(
    const int64_t tx_id,
    const int64_t commit_version);
  void fake_freeze_info();
  void prepare_query_param(const ObVersionRange &version_range);
  void prepare_scan_param(
    const ObITableReadInfo *read_info,
    const ObVersionRange &version_range,
    ObStoreCtx &store_ctx,
    ObTableIterParam &iter_param,
    ObTableAccessContext &context);
  static void create_tablet();
  static int prepare_rowscn_filter(
    const int64_t schema_rowkey_cnt,
    const int64_t filter_max_version,
    ObTabletMergeCtx &merge_context);
  static void co_major_merge(
    ObLocalArena &allocator,
    const ObCOMergeTestType type,
    ObCOTabletMergeCtx &merge_context,
    const int64_t task_id,
    const int64_t start_cg_idx,
    const int64_t end_cg_idx,
    const bool create_sstable = true,
    const bool no_column_store = false,
    const bool need_prepare_two_stage_ctx = true,
    const bool onle_use_row_store = false);

  static int create_cg_sstables(ObCOTabletMergeCtx &ctx, const int64_t start_cg_idx, const int64_t end_cg_idx)
  {
    int ret = OB_SUCCESS;
    for (int64_t i = start_cg_idx; OB_SUCC(ret) && i < end_cg_idx; ++i) {
      if (OB_FAIL(ctx.create_cg_sstable(i))) {
        LOG_WARN("failed to create cg sstable", K(ret), K(i));
      }
    }
    LOG_INFO("create cg sstable", K(ret), K(start_cg_idx), K(end_cg_idx));
    return ret;
  }
  ObStorageSchema table_merge_schema_;
  ObStoreCtx store_ctx_;
};

void TestMergeBasic::prepare_merge_context(
  const ObMergeType &merge_type,
  const bool is_full_merge,
  const ObVersionRange &trans_version_range,
  compaction::ObBasicTabletMergeCtx &merge_context)
{
  bool has_lob = false;
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  compaction::ObStaticMergeParam &static_param = merge_context.static_param_;
  static_param.ls_handle_ = ls_handle;

  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
  merge_context.tablet_handle_.assign(tablet_handle);

  table_merge_schema_.reset();
  OK(table_merge_schema_.init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
  static_param.schema_ = &table_merge_schema_;
  static_param.data_version_ = DATA_CURRENT_VERSION;
  static_param.is_full_merge_ = is_full_merge;
  static_param.merge_level_ = is_major_merge_type(merge_type) ? MICRO_BLOCK_MERGE_LEVEL : MACRO_BLOCK_MERGE_LEVEL;
  static_param.dag_param_.merge_type_ = merge_type;
  static_param.dag_param_.merge_version_ = is_major_merge_type(merge_type) ? trans_version_range.snapshot_version_ : 0;
  static_param.dag_param_.ls_id_ = ls_id_;
  static_param.dag_param_.tablet_id_ = tablet_id_;
  static_param.version_range_ = trans_version_range;
  const int64_t tables_count = static_param.tables_handle_.get_count();
  static_param.scn_range_.start_scn_ = static_param.tables_handle_.get_table(0)->get_start_scn();
  static_param.scn_range_.end_scn_ = static_param.tables_handle_.get_table(tables_count - 1)->get_end_scn();
  static_param.merge_scn_ = static_param.scn_range_.end_scn_;
  static_param.rec_scn_.set_min();
  OK(static_param.init_merge_version_range(trans_version_range));
  OK(static_param.init_major_sstable_count());
}

template <typename T>
void TestMergeBasic::prepare_merge_context(
  const ObMergeType &merge_type,
  const bool is_full_merge,
  const ObVersionRange &trans_version_range,
  ObTabletMergeDag *merge_dag,
  T &merge_context,
  const bool is_delete_insert_merge)
{
  TestMergeBasic::prepare_merge_context(merge_type, is_full_merge, trans_version_range, merge_context);
  bool unused_flag = false;
  merge_context.static_param_.is_delete_insert_merge_ = is_delete_insert_merge;
  merge_context.merge_dag_ = merge_dag;
  merge_context.static_param_.for_unittest_ = true;
  ASSERT_EQ(OB_SUCCESS, merge_context.build_ctx_after_init(unused_flag));
}

void TestMergeBasic::prepare_co_major_merge_context(
  const ObMergeType &merge_type,
  const bool is_full_merge,
  const ObVersionRange &trans_version_range,
  ObTabletMergeDag *merge_dag,
  ObCOTabletMergeCtx &merge_context,
  const bool is_delete_insert_merge)
{
  merge_context.static_param_.co_static_param_.co_major_merge_strategy_.set(false/*build_all_cg_only*/, false/*only_use_row_store*/);
  TestMergeBasic::prepare_merge_context(merge_type, is_full_merge, trans_version_range, merge_dag, merge_context, is_delete_insert_merge);
  ObCOMergeDagParam *dag_param = static_cast<ObCOMergeDagParam *>(&merge_context.static_param_.dag_param_);
  dag_param->compat_mode_ = lib::Worker::CompatMode::MYSQL;
  int32_t base_cg_idx = -1;
  ASSERT_EQ(OB_SUCCESS, merge_context.get_schema()->get_base_rowkey_column_group_index(base_cg_idx));
  merge_context.base_rowkey_cg_idx_ = base_cg_idx;
}

void TestMergeBasic::get_tx_table_guard(ObTxTableGuard &tx_table_guard)
{
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tx_table_guard(tx_table_guard));
}

void TestMergeBasic::insert_tx_data(
  const int64_t input_tx_id,
  const int64_t commit_version)
{
  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());
  ObTxData *tx_data = new ObTxData();
  transaction::ObTransID tx_id = input_tx_id;

  // fill in data
  tx_data->tx_id_ = tx_id;
  tx_data->commit_version_.convert_for_tx(commit_version);
  tx_data->start_scn_.convert_for_tx(tx_id);
  tx_data->end_scn_ = tx_data->commit_version_;
  if (INT64_MAX == commit_version) {
    tx_data->state_ = ObTxData::ABORT;
  } else if (commit_version > 0) {
    tx_data->state_ = ObTxData::COMMIT;
  } else {
    tx_data->state_ = ObTxData::RUNNING;
  }

  ASSERT_EQ(OB_SUCCESS, tx_table->insert(tx_data));
  delete tx_data;
}

void TestMergeBasic::fake_freeze_info()
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

void TestMergeBasic::prepare_query_param(
  const ObVersionRange &version_range)
{
  context_.reset();
  prepare_scan_param(&full_read_info_, version_range, store_ctx_, iter_param_, context_);
}

void TestMergeBasic::prepare_scan_param(
    const ObITableReadInfo *read_info,
    const ObVersionRange &version_range,
    ObStoreCtx &store_ctx,
    ObTableIterParam &iter_param,
    ObTableAccessContext &context)
{
  context.reset();
  ObLSID ls_id(ls_id_);
  iter_param.table_id_ = table_id_;
  iter_param.tablet_id_ = tablet_id_;
  iter_param.read_info_ = read_info;
  iter_param.out_cols_project_ = nullptr;
  iter_param.is_same_schema_column_ = true;
  iter_param.has_virtual_columns_ = false;
  iter_param.vectorized_enabled_ = false;
  ASSERT_EQ(OB_SUCCESS,
            store_ctx.init_for_read(ls_id,
                                    iter_param.tablet_id_,
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
            context.init(query_flag,
                          store_ctx,
                          allocator_,
                          allocator_,
                          version_range));
  context.limit_param_ = nullptr;
}

void TestMergeBasic::create_tablet()
{
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

int TestMergeBasic::prepare_rowscn_filter(
  const int64_t schema_rowkey_cnt,
  const int64_t filter_max_version,
  ObTabletMergeCtx &merge_context) {
  int ret = OB_SUCCESS;
  const ObICompactionFilter::CompactionFilterType filter_type = compaction::ObICompactionFilter::ROWSCN_FILTER;
  if (OB_UNLIKELY(schema_rowkey_cnt <= 0 || filter_max_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(schema_rowkey_cnt), K(filter_max_version));
  } else if (OB_FAIL(ObCompactionFilterFactory::alloc_compaction_filter<ObRowscnFilter>(
        allocator_,
        merge_context.filter_ctx_.compaction_filter_,
        filter_max_version,
        schema_rowkey_cnt,
        filter_type))) {
      LOG_WARN("failed to alloc rowscn filter", K(ret), K(filter_max_version), K(schema_rowkey_cnt));
  }
  return ret;
}

void TestMergeBasic::co_major_merge(
  ObLocalArena &allocator,
  const ObCOMergeTestType type,
  ObCOTabletMergeCtx &merge_context,
  const int64_t task_id,
  const int64_t start_cg_idx,
  const int64_t end_cg_idx,
  const bool create_sstable/* = true*/,
  const bool no_column_store/* = false*/,
  const bool need_prepare_two_stage_ctx/* = true*/,
  const bool onle_use_row_store/* = false*/)
{
  OK(merge_context.prepare_index_builder(start_cg_idx, end_cg_idx));
  if (need_replay_base(type)) {
    merge_context.need_replay_base_directly_ = 1;
  }
  if (ObCOMergeTestType::NORMAL == type || ObCOMergeTestType::NORMAL_WITH_BASE_REPLAY == type) {
    merge_context.merge_log_storage_ = 0;
    ObCOMergeLogReplayer replayer(allocator, merge_context.static_param_, start_cg_idx, end_cg_idx);
    ASSERT_EQ(OB_SUCCESS, replayer.init(merge_context, task_id));
    ASSERT_EQ(OB_SUCCESS, replayer.replay_merge_log());
    replayer.reset();
  } else {
    if (need_prepare_two_stage_ctx) {
      merge_context.merge_log_storage_ = 1;
      if (!no_column_store && is_column_tmp_file_test_type(type)) {
        merge_context.merge_log_storage_ = 2;
      }
      ASSERT_EQ(OB_SUCCESS, merge_context.prepare_two_stage_ctx());
    }
    // build merge log
    ObCOMergeLogPersister persister(allocator);
    ASSERT_EQ(OB_SUCCESS, persister.init(merge_context, task_id));
    ASSERT_EQ(OB_SUCCESS, persister.persist_merge_log());
    // replay merge log
    if (merge_context.is_using_column_tmp_file()) {
      for (int64_t i = start_cg_idx; i < end_cg_idx; ++i) {
        ObCOMergeLogReplayer replayer(allocator, merge_context.static_param_, i, i + 1, onle_use_row_store);
        ASSERT_EQ(OB_SUCCESS, replayer.init(merge_context, task_id));
        ASSERT_EQ(OB_SUCCESS, replayer.replay_merge_log());
        replayer.reset();
      }
    } else {
      ObCOMergeLogReplayer replayer(allocator, merge_context.static_param_, start_cg_idx, end_cg_idx, onle_use_row_store);
      ASSERT_EQ(OB_SUCCESS, replayer.init(merge_context, task_id));
      ASSERT_EQ(OB_SUCCESS, replayer.replay_merge_log());
      replayer.reset();
    }
    persister.reset();
  }
  if (create_sstable) {
    OK(create_cg_sstables(merge_context, start_cg_idx, end_cg_idx));
  }
}

ObSEArray<ObTxData, 8> TX_DATA_ARR;

void clear_tx_data()
{
  TX_DATA_ARR.reset();
};

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

}
}

#endif
