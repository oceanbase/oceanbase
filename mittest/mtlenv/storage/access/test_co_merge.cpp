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
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/test_tablet_helper.h"
#include "storage/column_store/ob_co_merge_dag.h"
#include "test_merge_basic.h"
#include "storage/column_store/ob_co_merge_log_operator.h"
#include "unittest/storage/test_schema_prepare.h"
#include "storage/compaction/ob_compaction_schedule_util.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace share;
using namespace blocksstable;
using namespace compaction;
using namespace memtable;
using namespace observer;
using namespace unittest;
using namespace memtable;
using namespace transaction;


ObCOTabletMergeCtx::~ObCOTabletMergeCtx()
{
  if (OB_NOT_NULL(cg_merge_info_array_)) {
    for (int i = 0; i < array_count_; ++i) {
      if (OB_NOT_NULL(cg_merge_info_array_[i])) {
        cg_merge_info_array_[i]->destroy();
        cg_merge_info_array_[i] = nullptr;
      }
    }
    mem_ctx_.free(cg_merge_info_array_);
    cg_merge_info_array_ = nullptr;
    merged_sstable_array_ = nullptr;
  }
}


namespace storage
{

static ObArenaAllocator allocator_;
static ObLocalArena merger_allocator_("TestArena", OB_MALLOC_NORMAL_BLOCK_SIZE);

void close_builder_and_prepare_sstable(
    const blocksstable::ObDataStoreDesc &data_store_desc,
    const ObITable::TableKey &table_key,
    const ObTableSchema &table_schema,
    const int64_t column_group_cnt,
    ObSSTableIndexBuilder &index_builder,
    ObTableHandleV2 &table_handle,
    bool is_all_cg_base = true)
{
  ObSSTableMergeRes res;
  OK(index_builder.close(res));
  ObIndexTreeRootBlockDesc root_desc;
  root_desc = res.root_desc_;
  ASSERT_TRUE(root_desc.is_valid());


  ObTabletCreateSSTableParam param;
  param.table_key_ = table_key;
  param.co_base_type_ = is_all_cg_base ? ObCOSSTableBaseType::ALL_CG_TYPE : ObCOSSTableBaseType::ROWKEY_CG_TYPE;
  param.rec_scn_.set_min();
  param.column_group_cnt_ = column_group_cnt;
  param.schema_version_ = ObMultiVersionSSTableTest::SCHEMA_VERSION;
  param.create_snapshot_version_ = 0;
  param.progressive_merge_round_ = 0;
  param.progressive_merge_step_ = 0;
  param.table_mode_ = table_schema.get_table_mode_struct();
  param.index_type_ = table_schema.get_index_type();
  param.rowkey_column_cnt_ =  data_store_desc.is_cg() ? 0 : table_schema.get_rowkey_column_num()
      + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();

  ObSSTableMergeRes::fill_addr_and_data(res.root_desc_,
                                        param.root_block_addr_, param.root_block_data_);
  ObSSTableMergeRes::fill_addr_and_data(res.data_root_desc_,
                                        param.data_block_macro_meta_addr_, param.data_block_macro_meta_);
  param.is_meta_root_ = res.data_root_desc_.is_meta_root_;
  param.root_row_store_type_ = res.root_row_store_type_;
  param.latest_row_store_type_ = res.root_row_store_type_;
  param.data_index_tree_height_ = res.root_desc_.height_;
  param.index_blocks_cnt_ = res.index_blocks_cnt_;
  param.data_blocks_cnt_ = res.data_blocks_cnt_;
  param.micro_block_cnt_ = res.micro_block_cnt_;
  param.use_old_macro_block_count_ = res.use_old_macro_block_count_;
  param.row_count_ = res.row_count_;
  param.column_cnt_ = res.data_column_cnt_;
  param.data_checksum_ = res.data_checksum_;
  param.occupy_size_ = res.occupy_size_;
  param.original_size_ = res.original_size_;
  param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param.max_merged_trans_version_ = res.max_merged_trans_version_;
  param.contain_uncommitted_row_ = res.contain_uncommitted_row_;
  param.compressor_type_ = res.compressor_type_;
  param.encrypt_id_ = res.encrypt_id_;
  param.master_key_id_ = res.master_key_id_;
  param.nested_size_ = res.nested_size_;
  param.nested_offset_ = res.nested_offset_;
  ASSERT_EQ(OB_SUCCESS, param.data_block_ids_.assign(res.data_block_ids_));
  ASSERT_EQ(OB_SUCCESS, param.other_block_ids_.assign(res.other_block_ids_));
  param.table_backup_flag_.reset();
  param.table_shared_flag_.reset();
  param.tx_data_recycle_scn_.set_min();
  param.sstable_logic_seq_ = 0;
  param.recycle_version_ = 0;
  param.root_macro_seq_ = 0;
  param.full_column_cnt_ = 0;
  param.is_co_table_without_cgs_ = false;
  param.co_base_snapshot_version_ = 0;
  param.ddl_scn_.set_min();
  param.filled_tx_scn_ = table_key.is_major_sstable() ? SCN::min_scn() : table_key.get_end_scn();

  if (is_major_merge_type(data_store_desc.get_merge_type())) {
    ASSERT_EQ(OB_SUCCESS, ObSSTableMergeRes::fill_column_checksum_for_empty_major(param.column_cnt_, param.column_checksums_));
  }
  if (data_store_desc.is_cg()) {
    OK(ObTabletCreateDeleteHelper::create_sstable(param, allocator_, table_handle));
  } else {
    OK(ObTabletCreateDeleteHelper::create_sstable<ObCOSSTableV2>(param, allocator_, table_handle));
  }
}

int get_col_ids(const ObTableSchema &table_schema, ObIArray<ObColDesc> &col_ids)
{
  return table_schema.get_store_column_ids(col_ids);
}

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

bool need_replay_base(const ObCOMergeTestType &type)
{
  return ObCOMergeTestType::NORMAL_WITH_BASE_REPLAY <= type && ObCOMergeTestType::MAX_TSET_TYPE > type;
}

bool is_normal_test_type(const ObCOMergeTestType &type)
{
  return ObCOMergeTestType::NORMAL == type || ObCOMergeTestType::NORMAL_WITH_BASE_REPLAY == type;
}

bool is_column_tmp_file_test_type(const ObCOMergeTestType &type)
{
  return ObCOMergeTestType::USE_COLUMN_TMP_FILE == type || ObCOMergeTestType::USE_COLUMN_TMP_FILE_WITH_BASE_REPLAY == type;
}

bool is_row_tmp_file_test_type(const ObCOMergeTestType &type)
{
  return ObCOMergeTestType::USE_ROW_TMP_FILE == type || ObCOMergeTestType::USE_ROW_TMP_FILE_WITH_BASE_REPLAY == type;
}

class TestCOMerge : public TestMergeBasic, public ::testing::WithParamInterface<ObCOMergeTestType>
{
public:
  static const int64_t MAX_PARALLEL_DEGREE = 10;
  TestCOMerge() :TestMergeBasic("testco_merge") {}
  virtual ~TestCOMerge() {}

  void SetUp();
  void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
  void prepare_query_param(const ObVersionRange &version_range);
  void prepare_merge_context(const ObMergeType &merge_type,
                             const bool is_full_merge,
                             const ObVersionRange &trans_version_range,
                             ObCOTabletMergeCtx &merge_context);
  void alloc_merge_infos(ObCOTabletMergeCtx &merge_context);
  void prepare_data(
      const int64_t micro_row_cnt,
      const int64_t macro_row_cnt,
      const int64_t row_count,
      ObMockIterator &data_iter,
      ObMacroBlockWriter &macro_writer,
      ObCOMergeProjector *projector);
  void prepare_co_sstable(
      const ObTableSchema &table_schema,
      const ObMergeType &merge_type,
      const int64_t snapshot_version,
      const int64_t multi_version_start,
      const int64_t* micro_row_cnt,
      const int64_t* macro_row_cnt,
      ObMockIterator &data_iter,
      ObTableHandleV2 &co_table_handle,
      const ObScnRange &scn_range,
      bool is_all_cg_base = true);
  void create_empty_data_co_sstable(const int64_t snapshot_version, ObTableHandleV2 &table_handle);

  void add_all_and_each_column_group();
  void add_rowkey_and_each_column_group();

  void prepare_scan_param(
    const ObITableReadInfo &cg_read_info,
    const ObVersionRange &version_range,
    ObStoreCtx &store_ctx,
    ObTableIterParam &iter_param,
    ObTableAccessContext &context);
  void construct_out_cols(ObIArray<int32_t> &out_cols_project, ObTableIterParam &iter_param);
  void init_co_sstable(storage::ObTablesHandleArray &merged_cg_tables_handle, const int64_t cnt)
  {
    int ret = OB_SUCCESS;
    common::ObArray<ObITable *> cg_tables;
    ObCOSSTableV2 *co_sstable = nullptr;
    for (int64_t i = 0; i < cnt; i++) {
      ObSSTable *merged_sstable = static_cast<ObSSTable *>(merged_cg_tables_handle.get_table(i));
      assert(merged_sstable);
      if (!merged_sstable->is_co_sstable()) {
        cg_tables.push_back(merged_sstable);
      } else {
        co_sstable = static_cast<ObCOSSTableV2 *>(merged_sstable);
      }
    }
    assert(co_sstable);
    ASSERT_EQ(OB_SUCCESS, co_sstable->fill_cg_sstables(cg_tables));
  }

  void get_cg_read_info(const ObColDesc &col_desc, const ObITableReadInfo *&cg_read_info)
  {
    int ret = OB_SUCCESS;
    cg_read_info_.reset();
    if (OB_FAIL(ObTenantCGReadInfoMgr::construct_cg_read_info(allocator_,
                                                              lib::is_oracle_mode(),
                                                              col_desc,
                                                              nullptr,
                                                              cg_read_info_))) {
      LOG_WARN("Fail to init cg read info", K(ret));
    } else {
      cg_read_info = &cg_read_info_;
    }
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  bool compare_datum_row(ObStoreRowIterator *res_iter, ObStoreRowIterator *sstable_iter);
public:
  ObCOMergeDagParam param_;
  ObCOMergeDagNet dag_net_;
  ObStoreCtx store_ctx_;
  ObTableReadInfo cg_read_info_;
};

void TestCOMerge::SetUpTestCase()
{
  ObMultiVersionSSTableTest::SetUpTestCase();
    //mock sequence no
  ObClockGenerator::init();

  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  // create tablet
  obrpc::ObBatchCreateTabletArg create_tablet_arg;
  share::schema::ObTableSchema table_schema;
  ASSERT_EQ(OB_SUCCESS, gen_create_tablet_arg(tenant_id_, ls_id, tablet_id, create_tablet_arg, 1, &table_schema));

  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema, allocator_));

  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);
  share::ObFreezeInfoList &info_list = mgr->freeze_info_mgr_.freeze_info_;
  info_list.reset();

  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, info_list.frozen_statuses_.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));
  info_list.latest_snapshot_gc_scn_.val_ = 2;

  // Enable major merge for CO merge tests
  compaction::ObBasicMergeScheduler *merge_scheduler = compaction::ObBasicMergeScheduler::get_merge_scheduler();
  if (nullptr != merge_scheduler) {
    merge_scheduler->resume_major_merge();
  }
}

void TestCOMerge::TearDownTestCase()
{
  ObMultiVersionSSTableTest::TearDownTestCase();
  // reset sequence no
  ObClockGenerator::destroy();
}

void TestCOMerge::SetUp()
{
  ObMultiVersionSSTableTest::SetUp();
  param_.compat_mode_ = lib::Worker::CompatMode::MYSQL;
}

void TestCOMerge::TearDown()
{
  ObMultiVersionSSTableTest::TearDown();
  TRANS_LOG(INFO, "teardown success");
}

void TestCOMerge::prepare_scan_param(
    const ObITableReadInfo &cg_read_info,
    const ObVersionRange &version_range,
    ObStoreCtx &store_ctx,
    ObTableIterParam &iter_param,
    ObTableAccessContext &context)
{
  context.reset();
  ObLSID ls_id(ls_id_);
  iter_param.table_id_ = table_id_;
  iter_param.tablet_id_ = tablet_id_;
  iter_param.read_info_ = &cg_read_info;
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

void TestCOMerge::construct_out_cols(ObIArray<int32_t> &out_cols_project, ObTableIterParam &iter_param)
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(nullptr, iter_param.out_cols_project_);
  int64_t store_schema_column_cnt = 0;
  int64_t col_index = OB_INVALID_INDEX_INT64;
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_store_column_count(store_schema_column_cnt, true));
  int64_t schema_rowkey_column_cnt = table_schema_.get_rowkey_column_num();
  for (int64_t idx = 0; idx < store_schema_column_cnt; ++idx) {
    col_index = idx < schema_rowkey_column_cnt ? idx : idx + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    ASSERT_EQ(OB_SUCCESS, out_cols_project.push_back(static_cast<int32_t>(col_index)));
  }
  iter_param.out_cols_project_ = &out_cols_project;
}

bool TestCOMerge::compare_datum_row(ObStoreRowIterator *res_iter, ObStoreRowIterator *sstable_iter)
{
  bool is_equal = true;
  int ret1 = OB_SUCCESS;
  int ret2 = OB_SUCCESS;
  int64_t idx = 0;
  OB_ASSERT(nullptr != res_iter);
  OB_ASSERT(nullptr != sstable_iter);
  const blocksstable::ObDatumRow *res_row = nullptr;
  const blocksstable::ObDatumRow *sstable_row = nullptr;
  while (is_equal) {
    ret1 = res_iter->get_next_row(res_row);
    ret2 = sstable_iter->get_next_row(sstable_row);
    if (ret1 == ret2) {
      if (common::OB_SUCCESS == ret1 && res_row && sstable_row) {
        is_equal = ObMockIterator::equals(*res_row, *sstable_row);
        if (res_row->trans_id_ != sstable_row->trans_id_) {
          STORAGE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "not equal trans_id", K(*res_row), K(res_row->trans_id_),
              K(*sstable_row), K(sstable_row->trans_id_));
          is_equal = false;
        }
      } else {
        // must be OB_ITER_END
        OB_ASSERT(common::OB_ITER_END == ret1);
        break;
      }
    } else {
      is_equal = false;
    }
    idx++;
  }
  if (!is_equal) {
    ObCStringHelper helper;
    STORAGE_LOG_RET(WARN, OB_ERR_UNEXPECTED, "iter is not equal",
                K(idx), K(ret1), K(ret2),
                "this_row", res_row ? helper.convert(*res_row): "null",
                "other_row", sstable_row ? helper.convert(*sstable_row) : "null");
  } else {
    ObCStringHelper helper;
    STORAGE_LOG(INFO, "iter is equal",
        K(idx), K(ret1), K(ret2),
        "this_row", res_row ? helper.convert(*res_row): "null",
        "other_row", sstable_row ? helper.convert(*sstable_row) : "null");
  }
  return is_equal;
}

void TestCOMerge::prepare_data(
    const int64_t micro_row_cnt,
    const int64_t macro_row_cnt,
    const int64_t column_count,
    ObMockIterator &data_iter,
    ObMacroBlockWriter &macro_writer,
    ObCOMergeProjector *projector)
{
  const ObStoreRow *row = nullptr;
  ObDatumRow datum_row;
  datum_row.init(allocator_, column_count);
  for (int64_t i = 0; i < data_iter.count(); i++) {
    OK(data_iter.get_row(i, row));
    ASSERT_TRUE(nullptr != row);
    datum_row.from_store_row(*row);
    if (nullptr != projector) {
      projector->project(datum_row);
    }
    const ObDatumRow &append_row = projector == nullptr ? datum_row : projector->get_project_row();
    ASSERT_EQ(OB_SUCCESS, macro_writer.append_row(append_row));
    if (macro_writer.micro_writer_->get_row_count() >= micro_row_cnt) {
      OK(macro_writer.build_micro_block());
    }
    if (macro_writer.macro_blocks_[macro_writer.current_index_].get_row_count() >= macro_row_cnt) {
      OK(macro_writer.try_switch_macro_block());
    }
  }

  OK(macro_writer.try_switch_macro_block());
}

void TestCOMerge::prepare_co_sstable(
  const ObTableSchema &table_schema,
  const ObMergeType &merge_type,
  const int64_t snapshot_version,
  const int64_t multi_version_start,
  const int64_t* micro_row_cnt,
  const int64_t* macro_row_cnt,
  ObMockIterator &data_iter,
  ObTableHandleV2 &co_table_handle,
  const ObScnRange &scn_range,
  bool is_all_cg_base)
{
  ObStorageSchema storage_schema;
  common::ObArray<ObITable *> cg_tables;
  ObITable *co_table = nullptr;
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));
  const common::ObIArray<ObStorageColumnGroupSchema> &cg_array = storage_schema.get_column_groups();

  for (int64_t i = 0; i < cg_array.count(); i++) {
    const ObStorageColumnGroupSchema &cg_schema = cg_array.at(i);
    ObCOMergeProjector projector;
    blocksstable::ObWholeDataStoreDesc data_store_desc;
    ObMacroBlockWriter macro_writer;
    ObSSTableIndexBuilder root_index_builder(false/* not need writer buffer */);
    ObCOMergeProjector *row_project = nullptr;
    ObTableHandleV2 *table_handle = nullptr;

    OK(data_store_desc.init(false/*is_ddl*/, table_schema,
                          ObLSID(ls_id_),
                          ObTabletID(tablet_id_),
                          merge_type,
                          snapshot_version,
                          DATA_CURRENT_VERSION,
                          table_schema.get_micro_index_clustered(),
                          0 /*tablet_transfer_seq*/,
                          0/*concurrent_cnt*/,
                          share::SCN::min_scn(), /*reorganization_scn*/
                          share::SCN::invalid_scn(), /*end_scn*/
                          &cg_schema,
                          i));
    ASSERT_EQ(OB_SUCCESS, root_index_builder.init(data_store_desc.get_desc()));
    data_store_desc.get_desc().sstable_index_builder_ = &root_index_builder;
    if (!cg_schema.is_all_column_group()) {
      OK(projector.init(cg_schema, allocator_));
      row_project = &projector;
    }
    ObMacroSeqParam seq_param;
    seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
    seq_param.start_ = 0;
    ObPreWarmerParam pre_warm_param(MEM_PRE_WARM);
    ObSSTablePrivateObjectCleaner cleaner;
    OK(macro_writer.open(data_store_desc.get_desc(), 0/*parallel_idx*/, seq_param/*start_seq*/, pre_warm_param, cleaner));
    prepare_data(micro_row_cnt[i], macro_row_cnt[i],
            table_schema.get_column_count(), data_iter, macro_writer, row_project);
    OK(macro_writer.close());
    // data write ctx has been moved to root_index_builder
    ASSERT_EQ(macro_writer.get_macro_block_write_ctx().get_macro_block_count(), 0);

    ObITable::TableKey table_key;
    table_key.tablet_id_ = ObTabletID(tablet_id_);
    table_key.table_type_ = cg_schema.has_multi_version_column() ? ObITable::COLUMN_ORIENTED_SSTABLE : ObITable::NORMAL_COLUMN_GROUP_SSTABLE;
    table_key.scn_range_ = scn_range;
    if (cg_schema.has_multi_version_column()) {
      table_key.column_group_idx_ = i;
    }
    ASSERT_NE(nullptr, table_handle = OB_NEWx(ObTableHandleV2, (&allocator_)));
    const int64_t cg_cnt = cg_schema.has_multi_version_column() ? cg_array.count() : 1;
    close_builder_and_prepare_sstable(data_store_desc.get_desc(), table_key, table_schema, cg_cnt, root_index_builder, *table_handle, is_all_cg_base);
    if (cg_schema.has_multi_version_column()) {
      co_table_handle.set_sstable(table_handle->get_table(), &allocator_);
    } else {
      OK(cg_tables.push_back(table_handle->get_table()));
    }
    //table_key.log_ts_range_ = 0;
  }

  for (int i = 0; i < cg_tables.count(); i++ ) {
    assert(cg_tables.at(i)->is_sstable());
  }
  ASSERT_EQ(OB_SUCCESS, static_cast<ObCOSSTableV2 *>(co_table_handle.get_table())->fill_cg_sstables(cg_tables));

}

void TestCOMerge::prepare_query_param(const ObVersionRange &version_range)
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

void TestCOMerge::alloc_merge_infos(ObCOTabletMergeCtx &merge_context)
{
  ASSERT_EQ(OB_SUCCESS, merge_context.init_tablet_merge_info());
}

void TestCOMerge::create_empty_data_co_sstable(const int64_t snapshot_version, ObTableHandleV2 &table_handle)
{
  ObStorageSchema storage_schema;
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_empty_sstable( allocator_,
                        storage_schema, ObTabletID(tablet_id_), snapshot_version, table_handle));
  LOG_INFO("succ to create_empty_sstable", K(table_schema_), K(storage_schema), KPC(table_handle.get_table()));
}

void TestCOMerge::add_all_and_each_column_group()
{
  unittest::TestSchemaPrepare::add_all_and_each_column_group(allocator_, table_schema_);
}

void TestCOMerge::add_rowkey_and_each_column_group()
{
  unittest::TestSchemaPrepare::add_rowkey_and_each_column_group(allocator_, table_schema_);
}

void TestCOMerge::prepare_merge_context(const ObMergeType &merge_type,
                                        const bool is_full_merge,
                                        const ObVersionRange &trans_version_range,
                                        ObCOTabletMergeCtx &merge_context)
{
  int32_t base_cg_idx = -1;
  TestMergeBasic::prepare_merge_context(merge_type, is_full_merge, trans_version_range, merge_context);
  merge_context.static_param_.co_static_param_.co_major_merge_type_ = ObCOMajorMergePolicy::BUILD_COLUMN_STORE_MERGE;
  merge_context.static_param_.data_version_ = DATA_VERSION_4_3_0_0;
  merge_context.static_param_.dag_param_.merge_version_ = trans_version_range.snapshot_version_;
  ASSERT_EQ(OB_SUCCESS, merge_context.cal_merge_param());
  ASSERT_EQ(OB_SUCCESS, merge_context.init_parallel_merge_ctx());
  ASSERT_EQ(OB_SUCCESS, merge_context.static_param_.init_static_info(merge_context.tablet_handle_));
  ASSERT_EQ(OB_SUCCESS, merge_context.init_static_desc());
  ASSERT_EQ(OB_SUCCESS, merge_context.init_read_info());
  ASSERT_EQ(OB_SUCCESS, merge_context.init_tablet_merge_info());
  ASSERT_EQ(OB_SUCCESS, merge_context.get_schema()->get_base_rowkey_column_group_index(base_cg_idx));
  merge_context.base_rowkey_cg_idx_ = base_cg_idx;
  ObCOMergeDagParam *dag_param = static_cast<ObCOMergeDagParam *>(&merge_context.static_param_.dag_param_);
  dag_param->compat_mode_ = lib::Worker::CompatMode::MYSQL;
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

void mix_type_test(
    const ObCOMergeTestType type,
    ObCOTabletMergeCtx &merge_context,
    const int64_t task_id,
    const int64_t start_cg_idx,
    const int64_t end_cg_idx,
    const bool no_column_store = false,
    const bool need_prepare_two_stage_ctx = true,
    const bool onle_use_row_store = false)
{
  if (need_replay_base(type)) {
    merge_context.need_replay_base_directly_ = 1;
  }
  if (ObCOMergeTestType::NORMAL == type || ObCOMergeTestType::NORMAL_WITH_BASE_REPLAY == type) {
    merge_context.merge_log_storage_ = 0;
    ObCOMergeLogReplayer replayer(merger_allocator_, merge_context.static_param_, start_cg_idx, end_cg_idx);
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
    ObCOMergeLogPersister persister(merger_allocator_);
    ASSERT_EQ(OB_SUCCESS, persister.init(merge_context, task_id));
    ASSERT_EQ(OB_SUCCESS, persister.persist_merge_log());
    // replay merge log
    if (merge_context.is_using_column_tmp_file()) {
      for (int64_t i = start_cg_idx; i < end_cg_idx; ++i) {
        ObCOMergeLogReplayer replayer(merger_allocator_, merge_context.static_param_, i, i + 1, onle_use_row_store);
        ASSERT_EQ(OB_SUCCESS, replayer.init(merge_context, task_id));
        ASSERT_EQ(OB_SUCCESS, replayer.replay_merge_log());
        replayer.reset();
      }
    } else {
      ObCOMergeLogReplayer replayer(merger_allocator_, merge_context.static_param_, start_cg_idx, end_cg_idx, onle_use_row_store);
      ASSERT_EQ(OB_SUCCESS, replayer.init(merge_context, task_id));
      ASSERT_EQ(OB_SUCCESS, replayer.replay_merge_log());
      replayer.reset();
    }
    persister.reset();
  }
}

TEST_P(TestCOMerge, test_merge_default_row_store_with_empty_major)
{
  ObCOMergeTestType type = GetParam();
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

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

  int64_t snapshot_version = 7;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  add_all_and_each_column_group();
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_one_macro(&micro_data[2], 1);
  prepare_data_end(handle1);

  ObTableHandleV2 empty_co_table_handle;

  ObStorageSchema storage_schema;
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_empty_sstable(allocator_, storage_schema,
                                            ObTabletID(tablet_id_), snapshot_version, empty_co_table_handle));

  merge_context.static_param_.tables_handle_.add_table(empty_co_table_handle);
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
  table_key_.scn_range_.start_scn_.convert_for_tx(10);
  table_key_.scn_range_.end_scn_.convert_for_tx(20);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;

  //prepare merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
  merge_context.array_count_ = 5;
  alloc_merge_infos(merge_context);
  OK(merge_context.prepare_index_builder(0, 1));

  mix_type_test(type, merge_context, 0, 0, 1);
  OK(create_cg_sstables(merge_context, 0, 1));

  ObSSTable *merged_sstable = static_cast<ObSSTable *>(merge_context.merged_cg_tables_handle_.get_table(0));
  ASSERT_NE(nullptr, merged_sstable);
  if (merged_sstable->is_co_sstable()) {
    static_cast<ObCOSSTableV2 *>(merged_sstable)->valid_for_cs_reading_ = true;
    static_cast<ObCOSSTableV2 *>(merged_sstable)->cs_meta_.column_group_cnt_ = 1;
  }

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "0        var1  -10      0        NULL      10     EXIST   \n"
      "1        var1  -8       0        2        2      EXIST   \n"
      "2        var1  -10      0        3        12     EXIST   \n"
      "3        var1  -10      0        NULL      13     EXIST   \n";

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
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, false/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
}

TEST_P(TestCOMerge, test_column_store_merge_with_empty_co_table)
{
  ObCOMergeTestType type = GetParam();
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "0        var1  -8       0        8        4    T_DML_UPDATE  EXIST   LF\n"
      "1        var1  -8       0        6        7    T_DML_INSERT  EXIST   CLF\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "2        var1  -8       MIN      3       2     T_DML_UPDATE EXIST   SCF\n"
      "2        var1  -8       0        3       NOP   T_DML_UPDATE EXIST   N\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "2        var1  -6       0        2       2     T_DML_INSERT EXIST   CL\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 7;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  ObArray<ObColDesc> col_ids;
  ASSERT_EQ(OB_SUCCESS, get_col_ids(table_schema_, col_ids));
  ASSERT_EQ(4, col_ids.count());
  add_all_and_each_column_group();

  init_tablet();
  // create co sstable
  ObTableHandleV2 co_table_handle;
  create_empty_data_co_sstable(snapshot_version, co_table_handle);
  ASSERT_EQ(5, static_cast<const ObCOSSTableV2 *>(co_table_handle.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle);

  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_one_macro(&micro_data[2], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;
 //prepare merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
  alloc_merge_infos(merge_context);
  OK(merge_context.prepare_index_builder(0, 4));
  mix_type_test(type, merge_context, 0, 0, 4);
  OK(create_cg_sstables(merge_context, 0, 4));

  const char *result1 =
      "  bigint  flag    multi_version_row_flag\n"
      "   8      EXIST   \n"
      "   6      EXIST   \n"
      "   3      EXIST   \n";

  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = nullptr;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;

  ObTableIterParam iter_param;
  ObTableAccessContext context;
  const ObITableReadInfo *cg_read_info = nullptr;
  ObStoreCtx store_ctx;
  get_cg_read_info(col_ids.at(2), cg_read_info);
  prepare_scan_param(*cg_read_info, trans_version_range, store_ctx, iter_param, context);
  ObSSTable *merged_sstable = static_cast<ObSSTable *>(merge_context.merged_cg_tables_handle_.get_table(3));
  STORAGE_LOG(INFO, "chaser debug sstable", K(ret), KPC(merged_sstable), K(merge_context.merged_cg_tables_handle_));
  ASSERT_NE(nullptr, merged_sstable);
  ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param, context, range, scanner));
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  ObMockDirectReadIterator sstable_iter;
  ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, *cg_read_info));
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, false/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
}

TEST_P(TestCOMerge, test_co_merge_with_twice_major)
{
  ObCOMergeTestType type = GetParam();
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint  dml           flag    multi_version_row_flag\n"
      "0        var1  -8       0        8       T_DML_UPDATE  EXIST   LF\n"
      "1        var1  -8       0        NOP       T_DML_INSERT  EXIST   CLF\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  dml           flag    multi_version_row_flag\n"
      "4        var1  -8       MIN      3       T_DML_UPDATE EXIST   SCF\n"
      "4        var1  -8       0        3       T_DML_UPDATE EXIST   N\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint   dml        flag    multi_version_row_flag\n"
      "4        var1  -6       0        2       T_DML_INSERT EXIST   CL\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 7;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(7);
  //prepare table schema
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  ObArray<ObColDesc> col_ids;
  ASSERT_EQ(OB_SUCCESS, get_col_ids(table_schema_, col_ids));
  ASSERT_EQ(3, col_ids.count());
  add_all_and_each_column_group();
  init_tablet();

  // create co sstable
  ObTableHandleV2 co_table_handle;
  create_empty_data_co_sstable(snapshot_version, co_table_handle);
  ASSERT_EQ(4, static_cast<const ObCOSSTableV2 *>(co_table_handle.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle);

  ObTableHandleV2 handle1;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_one_macro(&micro_data[2], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 9;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;

  //prepare merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
  merge_context.array_count_ = 4;
  alloc_merge_infos(merge_context);

  OK(merge_context.prepare_index_builder(0, 4));

  mix_type_test(type, merge_context, 0, 0, 4);
  OK(create_cg_sstables(merge_context, 0, 4));
  ASSERT_EQ(4, merge_context.merged_cg_tables_handle_.get_count());

  const char *result[4];
  result[0] =
      "bigint   var   bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "0        var1  -8       0        8        EXIST   \n"
      "1        var1  -8       0      NULL        EXIST  \n"
      "4        var1  -8       0        3        EXIST   \n";
  result[1] =
      "bigint    flag    multi_version_row_flag\n"
      "0         EXIST   \n"
      "1         EXIST   \n"
      "4         EXIST   \n";
  result[2] =
      "var          flag    multi_version_row_flag\n"
      "var1         EXIST   \n"
      "var1         EXIST   \n"
      "var1         EXIST   \n";
  result[3] =
      "bigint    flag    multi_version_row_flag\n"
      "8         EXIST   \n"
      "NULL      EXIST   \n"
      "3         EXIST   \n";

  ObTableHandleV2 new_co_table_handle;
  init_co_sstable(merge_context.merged_cg_tables_handle_, 4);
  for (int64_t i = 0; i < 4; i++) {
    ObDatumRange range;
    range.set_whole_range();
    trans_version_range.base_version_ = 1;
    trans_version_range.multi_version_start_ = 1;
    trans_version_range.snapshot_version_ = INT64_MAX;

    ObTableIterParam iter_param;
    ObTableAccessContext context;
    const ObITableReadInfo *cg_read_info = nullptr;
    ObStoreCtx store_ctx;

    ObSSTable *merged_sstable = static_cast<ObSSTable *>(merge_context.merged_cg_tables_handle_.get_table(i));
    ASSERT_NE(nullptr, merged_sstable);
    if (i > 0) {
      get_cg_read_info(col_ids.at(i - 1), cg_read_info);
    } else {
      cg_read_info = &full_read_info_;
    }
    if (merged_sstable->is_co_sstable()) {
      merge_context.merged_cg_tables_handle_.get_table(i, new_co_table_handle);
    }
    ObStoreRowIterator *scanner = nullptr;
    ObMockDirectReadIterator sstable_iter;
    prepare_scan_param(*cg_read_info, trans_version_range, store_ctx, iter_param, context);
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param, context, range, scanner));
    ASSERT_NE(nullptr, scanner);
    ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, *cg_read_info));

    ObMockIterator res_iter;
    res_iter.reset();
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result[i]));
    bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, false/*cmp multi version row flag*/);
    ASSERT_TRUE(is_equal);
    scanner->~ObStoreRowIterator();
  }
  handle1.reset();

  ObCOTabletMergeCtx new_merge_context(dag_net_, param_, allocator_);
  new_merge_context.static_param_.tables_handle_.add_table(new_co_table_handle);

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint      dml            flag    multi_version_row_flag\n"
      "0        var1  -10       0        nop      T_DML_UPDATE   EXIST        LF\n"
      "1        var1  -10       0        1        T_DML_UPDATE   EXIST        LF\n"
      "3        var1  -10       0        1        T_DML_UPDATE   EXIST        LF\n"
      "5        var1  -10       0        1        T_DML_UPDATE   EXIST        LF\n";

  snapshot_version = 20;
  table_key_.scn_range_.start_scn_.convert_for_tx(10);
  table_key_.scn_range_.end_scn_.convert_for_tx(20);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  new_merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;

  //prepare new merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, new_merge_context);
  new_merge_context.array_count_ = 4;
  alloc_merge_infos(new_merge_context);
  OK(new_merge_context.prepare_index_builder(0, 4));
  mix_type_test(type, new_merge_context, 0, 0, 4);
  OK(create_cg_sstables(new_merge_context, 0, 4));

  const char *new_result[4];
  new_result[0] =
      "bigint   var   bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "0        var1  -10       0        8       EXIST   \n"
      "1        var1  -10        0        1      EXIST   \n"
      "3        var1  -10       0        1       EXIST   \n"
      "4        var1  -8        0        3       EXIST   \n"
      "5        var1  -10       0        1       EXIST   \n";
  new_result[1] =
      "bigint    flag    multi_version_row_flag\n"
      "0         EXIST   \n"
      "1         EXIST   \n"
      "3         EXIST   \n"
      "4         EXIST   \n"
      "5         EXIST   \n";
  new_result[2] =
      "var          flag    multi_version_row_flag\n"
      "var1         EXIST   \n"
      "var1         EXIST   \n"
      "var1         EXIST   \n"
      "var1         EXIST   \n"
      "var1         EXIST   \n";
  new_result[3] =
      "bigint    flag    multi_version_row_flag\n"
      "8         EXIST   \n"
      "1         EXIST   \n"
      "1         EXIST   \n"
      "3         EXIST   \n"
      "1         EXIST   \n";

  init_co_sstable(new_merge_context.merged_cg_tables_handle_, 4);
  for (int64_t i = 0; i < 4; i++) {
    ObDatumRange range;
    range.set_whole_range();
    trans_version_range.base_version_ = 1;
    trans_version_range.multi_version_start_ = 1;
    trans_version_range.snapshot_version_ = INT64_MAX;

    ObTableIterParam iter_param;
    ObTableAccessContext context;
    const ObITableReadInfo *cg_read_info = nullptr;
    ObStoreCtx store_ctx;

    ObSSTable *merged_sstable = static_cast<ObSSTable *>(new_merge_context.merged_cg_tables_handle_.get_table(i));
    ASSERT_NE(nullptr, merged_sstable);
    if (i > 0) {
      get_cg_read_info(col_ids.at(i - 1), cg_read_info);
    } else {
      cg_read_info = &full_read_info_;
    }

    ObStoreRowIterator *scanner = nullptr;
    ObMockDirectReadIterator sstable_iter;
    prepare_scan_param(*cg_read_info, trans_version_range, store_ctx, iter_param, context);
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param, context, range, scanner));
    ASSERT_NE(nullptr, scanner);
    ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, *cg_read_info));

    ObMockIterator res_iter;
    res_iter.reset();
    ASSERT_EQ(OB_SUCCESS, res_iter.from(new_result[i]));
    bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, false/*cmp multi version row flag*/);
    ASSERT_TRUE(is_equal);
    scanner->~ObStoreRowIterator();
  }
}

TEST_P(TestCOMerge, test_merge_range)
{
  ObCOMergeTestType type = GetParam();
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  const char *co_table_data[1];
  co_table_data[0]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "0          -8       0        8        EXIST   \n"
      "1          -8       0      NULL        EXIST  \n"
      "3          -8       0        3        EXIST   \n"
      "4          -8       0        2        EXIST   \n"
      "6          -8       0        3        EXIST   \n"
      "7          -8       0        3        EXIST   \n"
      "9          -8       0        3        EXIST   \n"
      "12         -8       0        3        EXIST   \n";

  const char *micro_data1[1];
  micro_data1[0] =
      "bigint     bigint   bigint   bigint  dml           flag    multi_version_row_flag\n"
      "0          -13       0        9         T_DML_UPDATE  EXIST   CLF\n"
      "1          -11      0        NOP       T_DML_INSERT  EXIST   CLF\n"
      "5          -10       0        NOP       T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 1;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);

  //prepare table schema
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version);
  ObArray<ObColDesc> col_ids;
  ASSERT_EQ(OB_SUCCESS, get_col_ids(table_schema_, col_ids));
  ASSERT_EQ(2, col_ids.count());
  add_all_and_each_column_group();
  init_tablet();

  // create co sstable
  ObMockIterator data_iter;
  data_iter.reset();
  OK(data_iter.from(co_table_data[0]));
  ObTableHandleV2 co_table_handle;
  const int64_t micro_row_count[4] = {20, 2, 2, 1};
  const int64_t macro_row_count[4] = {30, 4, 4, 3};
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle, scn_range);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle);

  ObTableHandleV2 handle1;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");


  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;

  //prepare merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
  merge_context.array_count_ = 3;
  alloc_merge_infos(merge_context);
  OK(merge_context.prepare_index_builder(0, 3));

  //prepare merge_range
  ObDatumRowkey start_key, end_key;
  void *buf = allocator_.alloc(sizeof(ObStorageDatum) * 2);
  ObStorageDatum *datums = new (buf) ObStorageDatum[2]();
  datums[0].set_int(3);
  datums[1].set_int(6);
  start_key.datums_ = datums;
  start_key.datum_cnt_ = 1;
  end_key.datums_ = &datums[1];
  end_key.datum_cnt_ = 1;
  ObDatumRange merge_range;
  merge_range.reset();
  merge_range.set_start_key(start_key);
  merge_range.set_end_key(end_key);
  merge_range.set_left_closed();
  merge_range.set_right_closed();


  merge_context.parallel_merge_ctx_.range_array_.reset();
  OK(merge_context.parallel_merge_ctx_.range_array_.push_back(merge_range));

  mix_type_test(type, merge_context, 0, 0, 3);
  OK(create_cg_sstables(merge_context, 0, 3));
  ASSERT_EQ(3, merge_context.merged_cg_tables_handle_.get_count());

  const char *result[3];
  result[0] =
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "3          -8       0        3        EXIST   \n"
      "4          -8       0        2        EXIST   \n"
      "5          -10       0        NULL        EXIST   \n"
      "6          -8       0        3        EXIST   \n";
  result[1] =
      "bigint    flag    multi_version_row_flag\n"
      "3         EXIST   \n"
      "4         EXIST   \n"
      "5         EXIST   \n"
      "6         EXIST   \n";
  result[2] =
      "bigint    flag    multi_version_row_flag\n"
      "3         EXIST   \n"
      "2         EXIST   \n"
      "NULL         EXIST   \n"
      "3         EXIST   \n";

  init_co_sstable(merge_context.merged_cg_tables_handle_, 3);
  for (int64_t i = 0; i < 3; i++) {
    ObDatumRange range;
    range.set_whole_range();
    trans_version_range.base_version_ = 1;
    trans_version_range.multi_version_start_ = 1;
    trans_version_range.snapshot_version_ = INT64_MAX;

    ObTableIterParam iter_param;
    ObTableAccessContext context;
    const ObITableReadInfo *cg_read_info = nullptr;
    ObStoreCtx store_ctx;

    ObSSTable *merged_sstable = static_cast<ObSSTable *>(merge_context.merged_cg_tables_handle_.get_table(i));
    ASSERT_NE(nullptr, merged_sstable);
    if (i > 0) {
      get_cg_read_info(col_ids.at(i - 1), cg_read_info);
    } else {
      cg_read_info = &full_read_info_;
    }

    ObStoreRowIterator *scanner = nullptr;
    ObMockDirectReadIterator sstable_iter;
    prepare_scan_param(*cg_read_info, trans_version_range, store_ctx, iter_param, context);
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param, context, range, scanner));
    ASSERT_NE(nullptr, scanner);
    ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, *cg_read_info));

    ObMockIterator res_iter;
    res_iter.reset();
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result[i]));
    bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, false/*cmp multi version row flag*/);
    ASSERT_TRUE(is_equal);
    scanner->~ObStoreRowIterator();
  }
}

TEST_P(TestCOMerge, test_merge_range_with_open)
{
  ObCOMergeTestType type = GetParam();
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  const char *co_table_data[1];
  co_table_data[0]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "0          -8       0        8        EXIST   \n"
      "1          -8       0      NULL        EXIST  \n"
      "3          -8       0        3        EXIST   \n"
      "4          -8       0        2        EXIST   \n"
      "6          -8       0        3        EXIST   \n"
      "7          -8       0        3        EXIST   \n"
      "9          -8       0        3        EXIST   \n"
      "12         -8       0        3        EXIST   \n";

  const char *micro_data1[1];
  micro_data1[0] =
      "bigint     bigint   bigint   bigint  dml           flag    multi_version_row_flag\n"
      "0          -13       0        9         T_DML_UPDATE  EXIST   CLF\n"
      "1          -11      0        NOP       T_DML_INSERT  EXIST   CLF\n"
      "5          -10       0        NOP       T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 1;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);

  //prepare table schema
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version);
  ObArray<ObColDesc> col_ids;
  ASSERT_EQ(OB_SUCCESS, get_col_ids(table_schema_, col_ids));
  ASSERT_EQ(2, col_ids.count());
  add_all_and_each_column_group();
  init_tablet();

  // create co sstable
  ObMockIterator data_iter;
  data_iter.reset();
  OK(data_iter.from(co_table_data[0]));
  ObTableHandleV2 co_table_handle;
  const int64_t micro_row_count[4] = {20, 2, 2, 1};
  const int64_t macro_row_count[4] = {30, 4, 4, 3};
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle, scn_range);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle);

  ObTableHandleV2 handle1;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");


  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;

  //prepare merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
	merge_context.array_count_ = 3;
  alloc_merge_infos(merge_context);
  OK(merge_context.prepare_index_builder(0, 3));

  //prepare merge_range
  ObDatumRowkey start_key, end_key;
  void *buf = allocator_.alloc(sizeof(ObStorageDatum) * 2);
  ObStorageDatum *datums = new (buf) ObStorageDatum[2]();
  datums[0].set_int(3);
  datums[1].set_int(6);
  start_key.datums_ = datums;
  start_key.datum_cnt_ = 1;
  end_key.datums_ = &datums[1];
  end_key.datum_cnt_ = 1;
  ObDatumRange merge_range;
  merge_range.reset();
  merge_range.set_start_key(start_key);
  merge_range.set_end_key(end_key);
  merge_range.set_left_open();
  merge_range.set_right_open();


  merge_context.parallel_merge_ctx_.range_array_.reset();
  OK(merge_context.parallel_merge_ctx_.range_array_.push_back(merge_range));

  mix_type_test(type, merge_context, 0, 0, 3);
  OK(create_cg_sstables(merge_context, 0, 3));
  ASSERT_EQ(3, merge_context.merged_cg_tables_handle_.get_count());

  const char *result[3];
  result[0] =
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "4          -8       0        2        EXIST   \n"
      "5          -10       0        NULL        EXIST   \n";
  result[1] =
      "bigint    flag    multi_version_row_flag\n"
      "4         EXIST   \n"
      "5         EXIST   \n";
  result[2] =
      "bigint    flag    multi_version_row_flag\n"
      "2         EXIST   \n"
      "NULL         EXIST   \n";

  init_co_sstable(merge_context.merged_cg_tables_handle_, 3);
  for (int64_t i = 0; i < 3; i++) {
    ObDatumRange range;
    range.set_whole_range();
    trans_version_range.base_version_ = 1;
    trans_version_range.multi_version_start_ = 1;
    trans_version_range.snapshot_version_ = INT64_MAX;

    ObTableIterParam iter_param;
    ObTableAccessContext context;
    const ObITableReadInfo *cg_read_info = nullptr;
    ObStoreCtx store_ctx;

    ObSSTable *merged_sstable = static_cast<ObSSTable *>(merge_context.merged_cg_tables_handle_.get_table(i));
    ASSERT_NE(nullptr, merged_sstable);
    if (i > 0) {
      get_cg_read_info(col_ids.at(i - 1), cg_read_info);
    } else {
      cg_read_info = &full_read_info_;
    }

    ObStoreRowIterator *scanner = nullptr;
    ObMockDirectReadIterator sstable_iter;
    prepare_scan_param(*cg_read_info, trans_version_range, store_ctx, iter_param, context);
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param, context, range, scanner));
    ASSERT_NE(nullptr, scanner);
    ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, *cg_read_info));

    ObMockIterator res_iter;
    res_iter.reset();
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result[i]));
    bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, false/*cmp multi version row flag*/);
    ASSERT_TRUE(is_equal);
    scanner->~ObStoreRowIterator();
  }
}

TEST_P(TestCOMerge, test_merge_range_with_left_open)
{
  ObCOMergeTestType type = GetParam();
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  const char *co_table_data[1];
  co_table_data[0]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "0          -8       0        8        EXIST   \n"
      "1          -8       0      NULL        EXIST  \n"
      "3          -8       0        3        EXIST   \n"
      "4          -8       0        2        EXIST   \n"
      "6          -8       0        3        EXIST   \n"
      "7          -8       0        3        EXIST   \n"
      "9          -8       0        3        EXIST   \n"
      "12         -8       0        3        EXIST   \n";

  const char *micro_data1[1];
  micro_data1[0] =
      "bigint     bigint   bigint   bigint  dml           flag    multi_version_row_flag\n"
      "0          -13       0        9         T_DML_UPDATE  EXIST   CLF\n"
      "1          -11      0        NOP       T_DML_INSERT  EXIST   CLF\n"
      "5          -10       0        NOP       T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 1;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);

  //prepare table schema
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version);
  ObArray<ObColDesc> col_ids;
  ASSERT_EQ(OB_SUCCESS, get_col_ids(table_schema_, col_ids));
  ASSERT_EQ(2, col_ids.count());
  add_all_and_each_column_group();
  init_tablet();

  // create co sstable
  ObMockIterator data_iter;
  data_iter.reset();
  OK(data_iter.from(co_table_data[0]));
  ObTableHandleV2 co_table_handle;
  const int64_t micro_row_count[4] = {20, 2, 2, 1};
  const int64_t macro_row_count[4] = {30, 4, 4, 3};
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle, scn_range);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle);

  ObTableHandleV2 handle1;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");


  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;

  //prepare merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
	merge_context.array_count_ = 3;
  alloc_merge_infos(merge_context);
  OK(merge_context.prepare_index_builder(0, 3));

  //prepare merge_range
  ObDatumRowkey start_key, end_key;
  void *buf = allocator_.alloc(sizeof(ObStorageDatum) * 2);
  ObStorageDatum *datums = new (buf) ObStorageDatum[2]();
  datums[0].set_int(3);
  datums[1].set_int(6);
  start_key.datums_ = datums;
  start_key.datum_cnt_ = 1;
  end_key.datums_ = &datums[1];
  end_key.datum_cnt_ = 1;
  ObDatumRange merge_range;
  merge_range.reset();
  merge_range.set_start_key(start_key);
  merge_range.set_end_key(end_key);
  merge_range.set_left_open();
  merge_range.set_right_closed();


  merge_context.parallel_merge_ctx_.range_array_.reset();
  OK(merge_context.parallel_merge_ctx_.range_array_.push_back(merge_range));

  mix_type_test(type, merge_context, 0, 0, 3);
  OK(create_cg_sstables(merge_context, 0, 3));
  ASSERT_EQ(3, merge_context.merged_cg_tables_handle_.get_count());

  const char *result[3];
  result[0] =
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "4          -8       0        2        EXIST   \n"
      "5          -10       0        NULL        EXIST   \n"
      "6          -8       0        3        EXIST   \n";
  result[1] =
      "bigint    flag    multi_version_row_flag\n"
      "4         EXIST   \n"
      "5         EXIST   \n"
      "6         EXIST   \n";
  result[2] =
      "bigint    flag    multi_version_row_flag\n"
      "2         EXIST   \n"
      "NULL         EXIST   \n"
      "3         EXIST   \n";

  init_co_sstable(merge_context.merged_cg_tables_handle_, 3);
  for (int64_t i = 0; i < 3; i++) {
    ObDatumRange range;
    range.set_whole_range();
    trans_version_range.base_version_ = 1;
    trans_version_range.multi_version_start_ = 1;
    trans_version_range.snapshot_version_ = INT64_MAX;

    ObTableIterParam iter_param;
    ObTableAccessContext context;
    const ObITableReadInfo *cg_read_info = nullptr;
    ObStoreCtx store_ctx;

    ObSSTable *merged_sstable = static_cast<ObSSTable *>(merge_context.merged_cg_tables_handle_.get_table(i));
    ASSERT_NE(nullptr, merged_sstable);
    if (i > 0) {
      get_cg_read_info(col_ids.at(i - 1), cg_read_info);
    } else {
      cg_read_info = &full_read_info_;
    }

    ObStoreRowIterator *scanner = nullptr;
    ObMockDirectReadIterator sstable_iter;
    prepare_scan_param(*cg_read_info, trans_version_range, store_ctx, iter_param, context);
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param, context, range, scanner));
    ASSERT_NE(nullptr, scanner);
    ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, *cg_read_info));

    ObMockIterator res_iter;
    res_iter.reset();
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result[i]));
    bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, false/*cmp multi version row flag*/);
    ASSERT_TRUE(is_equal);
    scanner->~ObStoreRowIterator();
  }
}

TEST_P(TestCOMerge, test_merge_range_with_right_open)
{
  ObCOMergeTestType type = GetParam();
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  const char *co_table_data[1];
  co_table_data[0]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "0          -8       0        8        EXIST   \n"
      "1          -8       0      NULL        EXIST  \n"
      "3          -8       0        3        EXIST   \n"
      "4          -8       0        2        EXIST   \n"
      "6          -8       0        3        EXIST   \n"
      "7          -8       0        3        EXIST   \n"
      "9          -8       0        3        EXIST   \n"
      "12         -8       0        3        EXIST   \n";

  const char *micro_data1[1];
  micro_data1[0] =
      "bigint     bigint   bigint   bigint  dml           flag    multi_version_row_flag\n"
      "0          -13       0        9         T_DML_UPDATE  EXIST   CLF\n"
      "1          -11      0        NOP       T_DML_INSERT  EXIST   CLF\n"
      "5          -10       0        NOP       T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 1;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);

  //prepare table schema
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version);
  ObArray<ObColDesc> col_ids;
  ASSERT_EQ(OB_SUCCESS, get_col_ids(table_schema_, col_ids));
  ASSERT_EQ(2, col_ids.count());
  add_all_and_each_column_group();
  init_tablet();

  // create co sstable
  ObMockIterator data_iter;
  data_iter.reset();
  OK(data_iter.from(co_table_data[0]));
  ObTableHandleV2 co_table_handle;
  const int64_t micro_row_count[4] = {20, 2, 2, 1};
  const int64_t macro_row_count[4] = {30, 4, 4, 3};
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle, scn_range);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle);

  ObTableHandleV2 handle1;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");


  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;

  //prepare merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
	merge_context.array_count_ = 3;
  alloc_merge_infos(merge_context);
  OK(merge_context.prepare_index_builder(0, 3));

  //prepare merge_range
  ObDatumRowkey start_key, end_key;
  void *buf = allocator_.alloc(sizeof(ObStorageDatum) * 2);
  ObStorageDatum *datums = new (buf) ObStorageDatum[2]();
  datums[0].set_int(3);
  datums[1].set_int(6);
  start_key.datums_ = datums;
  start_key.datum_cnt_ = 1;
  end_key.datums_ = &datums[1];
  end_key.datum_cnt_ = 1;
  ObDatumRange merge_range;
  merge_range.reset();
  merge_range.set_start_key(start_key);
  merge_range.set_end_key(end_key);
  merge_range.set_left_closed();
  merge_range.set_right_open();


  merge_context.parallel_merge_ctx_.range_array_.reset();
  OK(merge_context.parallel_merge_ctx_.range_array_.push_back(merge_range));

  mix_type_test(type, merge_context, 0, 0, 3);
  OK(create_cg_sstables(merge_context, 0, 3));
  ASSERT_EQ(3, merge_context.merged_cg_tables_handle_.get_count());

  const char *result[3];
  result[0] =
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "3          -8       0        3        EXIST   \n"
      "4          -8       0        2        EXIST   \n"
      "5          -10       0        NULL        EXIST   \n";
  result[1] =
      "bigint    flag    multi_version_row_flag\n"
      "3         EXIST   \n"
      "4         EXIST   \n"
      "5         EXIST   \n";
  result[2] =
      "bigint    flag    multi_version_row_flag\n"
      "3         EXIST   \n"
      "2         EXIST   \n"
      "NULL         EXIST   \n";

  init_co_sstable(merge_context.merged_cg_tables_handle_, 3);
  for (int64_t i = 0; i < 3; i++) {
    ObDatumRange range;
    range.set_whole_range();
    trans_version_range.base_version_ = 1;
    trans_version_range.multi_version_start_ = 1;
    trans_version_range.snapshot_version_ = INT64_MAX;

    ObTableIterParam iter_param;
    ObTableAccessContext context;
    const ObITableReadInfo *cg_read_info = nullptr;
    ObStoreCtx store_ctx;

    ObSSTable *merged_sstable = static_cast<ObSSTable *>(merge_context.merged_cg_tables_handle_.get_table(i));
    ASSERT_NE(nullptr, merged_sstable);
    if (i > 0) {
      get_cg_read_info(col_ids.at(i - 1), cg_read_info);
    } else {
      cg_read_info = &full_read_info_;
    }

    ObStoreRowIterator *scanner = nullptr;
    ObMockDirectReadIterator sstable_iter;
    prepare_scan_param(*cg_read_info, trans_version_range, store_ctx, iter_param, context);
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param, context, range, scanner));
    ASSERT_NE(nullptr, scanner);
    ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, *cg_read_info));

    ObMockIterator res_iter;
    res_iter.reset();
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result[i]));
    bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, false/*cmp multi version row flag*/);
    ASSERT_TRUE(is_equal);
    scanner->~ObStoreRowIterator();
  }
}

TEST_P(TestCOMerge, test_merge_range_left_is_min)
{
  ObCOMergeTestType type = GetParam();
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  const char *co_table_data[1];
  co_table_data[0]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "0          -8       0        8        EXIST   \n"
      "1          -8       0      NULL        EXIST  \n"
      "3          -8       0        3        EXIST   \n"
      "4          -8       0        2        EXIST   \n"
      "6          -8       0        3        EXIST   \n"
      "7          -8       0        3        EXIST   \n"
      "9          -8       0        3        EXIST   \n"
      "12         -8       0        3        EXIST   \n";

  const char *micro_data1[1];
  micro_data1[0] =
      "bigint     bigint   bigint   bigint  dml           flag    multi_version_row_flag\n"
      "0          -13       0        9         T_DML_UPDATE  EXIST   CLF\n"
      "1          -11      0        NOP       T_DML_INSERT  EXIST   CLF\n"
      "5          -10       0        NOP       T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 1;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);

  //prepare table schema
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version);
  ObArray<ObColDesc> col_ids;
  ASSERT_EQ(OB_SUCCESS, get_col_ids(table_schema_, col_ids));
  ASSERT_EQ(2, col_ids.count());
  add_all_and_each_column_group();
  init_tablet();

  // create co sstable
  ObMockIterator data_iter;
  data_iter.reset();
  OK(data_iter.from(co_table_data[0]));
  ObTableHandleV2 co_table_handle;
  const int64_t micro_row_count[4] = {20, 2, 2, 1};
  const int64_t macro_row_count[4] = {30, 4, 4, 3};
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle, scn_range);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle);

  ObTableHandleV2 handle1;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");


  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;

  //prepare merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
	merge_context.array_count_ = 3;
  alloc_merge_infos(merge_context);
  OK(merge_context.prepare_index_builder(0, 3));

  //prepare merge_range
  ObDatumRowkey start_key, end_key;
  void *buf = allocator_.alloc(sizeof(ObStorageDatum) * 2);
  ObStorageDatum *datums = new (buf) ObStorageDatum[2]();
  datums[0].set_int(3);
  datums[1].set_int(6);
  start_key.datums_ = datums;
  start_key.datum_cnt_ = 1;
  end_key.datums_ = &datums[1];
  end_key.datum_cnt_ = 1;
  ObDatumRange merge_range;
  merge_range.reset();
  merge_range.start_key_.set_min_rowkey();
  merge_range.set_end_key(end_key);
  merge_range.set_left_open();
  merge_range.set_right_closed();


  merge_context.parallel_merge_ctx_.range_array_.reset();
  OK(merge_context.parallel_merge_ctx_.range_array_.push_back(merge_range));

  mix_type_test(type, merge_context, 0, 0, 3);
  OK(create_cg_sstables(merge_context, 0, 3));
  ASSERT_EQ(3, merge_context.merged_cg_tables_handle_.get_count());

  const char *result[3];
  result[0] =
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "0          -13       0        9        EXIST   \n"
      "1          -11       0      NULL        EXIST  \n"
      "3          -8       0        3        EXIST   \n"
      "4          -8       0        2        EXIST   \n"
      "5          -10       0        NULL        EXIST   \n"
      "6          -8       0        3        EXIST   \n";
  result[1] =
      "bigint    flag    multi_version_row_flag\n"
      "0         EXIST   \n"
      "1         EXIST   \n"
      "3         EXIST   \n"
      "4         EXIST   \n"
      "5         EXIST   \n"
      "6         EXIST   \n";
  result[2] =
      "bigint    flag    multi_version_row_flag\n"
      "9         EXIST   \n"
      "NULL         EXIST   \n"
      "3         EXIST   \n"
      "2         EXIST   \n"
      "NULL         EXIST   \n"
      "3         EXIST   \n";

  init_co_sstable(merge_context.merged_cg_tables_handle_, 3);
  for (int64_t i = 0; i < 3; i++) {
    ObDatumRange range;
    range.set_whole_range();
    trans_version_range.base_version_ = 1;
    trans_version_range.multi_version_start_ = 1;
    trans_version_range.snapshot_version_ = INT64_MAX;

    ObTableIterParam iter_param;
    ObTableAccessContext context;
    const ObITableReadInfo *cg_read_info = nullptr;
    ObStoreCtx store_ctx;

    ObSSTable *merged_sstable = static_cast<ObSSTable *>(merge_context.merged_cg_tables_handle_.get_table(i));
    ASSERT_NE(nullptr, merged_sstable);
    if (i > 0) {
      get_cg_read_info(col_ids.at(i - 1), cg_read_info);
    } else {
      cg_read_info = &full_read_info_;
    }

    ObStoreRowIterator *scanner = nullptr;
    ObMockDirectReadIterator sstable_iter;
    prepare_scan_param(*cg_read_info, trans_version_range, store_ctx, iter_param, context);
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param, context, range, scanner));
    ASSERT_NE(nullptr, scanner);
    ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, *cg_read_info));

    ObMockIterator res_iter;
    res_iter.reset();
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result[i]));
    bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, false/*cmp multi version row flag*/);
    ASSERT_TRUE(is_equal);
    scanner->~ObStoreRowIterator();
  }
}

TEST_P(TestCOMerge, test_merge_range_with_right_max)
{
  ObCOMergeTestType type = GetParam();
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  const char *co_table_data[1];
  co_table_data[0]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "0          -8       0        8        EXIST   \n"
      "1          -8       0      NULL        EXIST  \n"
      "3          -8       0        3        EXIST   \n"
      "4          -8       0        2        EXIST   \n"
      "6          -8       0        3        EXIST   \n"
      "7          -8       0        3        EXIST   \n"
      "9          -8       0        3        EXIST   \n"
      "12         -8       0        3        EXIST   \n";

  const char *micro_data1[1];
  micro_data1[0] =
      "bigint     bigint   bigint   bigint  dml           flag    multi_version_row_flag\n"
      "0          -13       0        9         T_DML_UPDATE  EXIST   CLF\n"
      "1          -11      0        NOP       T_DML_INSERT  EXIST   CLF\n"
      "5          -10       0        NOP       T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 1;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);

  //prepare table schema
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version);
  ObArray<ObColDesc> col_ids;
  ASSERT_EQ(OB_SUCCESS, get_col_ids(table_schema_, col_ids));
  ASSERT_EQ(2, col_ids.count());
  add_all_and_each_column_group();
  init_tablet();

  // create co sstable
  ObMockIterator data_iter;
  data_iter.reset();
  OK(data_iter.from(co_table_data[0]));
  ObTableHandleV2 co_table_handle;
  const int64_t micro_row_count[4] = {20, 2, 2, 1};
  const int64_t macro_row_count[4] = {30, 4, 4, 3};
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle, scn_range);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle);

  ObTableHandleV2 handle1;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");


  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;

  //prepare merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
	merge_context.array_count_ = 3;
  alloc_merge_infos(merge_context);
  OK(merge_context.prepare_index_builder(0, 3));

  //prepare merge_range
  ObDatumRowkey start_key, end_key;
  void *buf = allocator_.alloc(sizeof(ObStorageDatum) * 2);
  ObStorageDatum *datums = new (buf) ObStorageDatum[2]();
  datums[0].set_int(3);
  datums[1].set_int(6);
  start_key.datums_ = datums;
  start_key.datum_cnt_ = 1;
  end_key.datums_ = &datums[1];
  end_key.datum_cnt_ = 1;
  ObDatumRange merge_range;
  merge_range.reset();
  merge_range.set_start_key(start_key);
  merge_range.end_key_.set_max_rowkey();
  merge_range.set_left_closed();
  merge_range.set_right_open();


  merge_context.parallel_merge_ctx_.range_array_.reset();
  OK(merge_context.parallel_merge_ctx_.range_array_.push_back(merge_range));

  mix_type_test(type, merge_context, 0, 0, 3);
  OK(create_cg_sstables(merge_context, 0, 3));
  ASSERT_EQ(3, merge_context.merged_cg_tables_handle_.get_count());

  const char *result[3];
  result[0] =
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "3          -8       0        3        EXIST   \n"
      "4          -8       0        2        EXIST   \n"
      "5          -10       0        NULL        EXIST   \n"
      "6          -8       0        3        EXIST   \n"
      "7          -8       0        3        EXIST   \n"
      "9          -8       0        3        EXIST   \n"
      "12         -8       0        3        EXIST   \n";
  result[1] =
      "bigint    flag    multi_version_row_flag\n"
      "3         EXIST   \n"
      "4         EXIST   \n"
      "5         EXIST   \n"
      "6         EXIST   \n"
      "7         EXIST   \n"
      "9         EXIST   \n"
      "12         EXIST   \n";
  result[2] =
      "bigint    flag    multi_version_row_flag\n"
      "3         EXIST   \n"
      "2         EXIST   \n"
      "NULL      EXIST   \n"
      "3         EXIST   \n"
      "3         EXIST   \n"
      "3         EXIST   \n"
      "3         EXIST   \n";

  init_co_sstable(merge_context.merged_cg_tables_handle_, 3);
  for (int64_t i = 0; i < 3; i++) {
    ObDatumRange range;
    range.set_whole_range();
    trans_version_range.base_version_ = 1;
    trans_version_range.multi_version_start_ = 1;
    trans_version_range.snapshot_version_ = INT64_MAX;

    ObTableIterParam iter_param;
    ObTableAccessContext context;
    const ObITableReadInfo *cg_read_info = nullptr;
    ObStoreCtx store_ctx;

    ObSSTable *merged_sstable = static_cast<ObSSTable *>(merge_context.merged_cg_tables_handle_.get_table(i));
    ASSERT_NE(nullptr, merged_sstable);
    if (i > 0) {
      get_cg_read_info(col_ids.at(i - 1), cg_read_info);
    } else {
      cg_read_info = &full_read_info_;
    }

    ObStoreRowIterator *scanner = nullptr;
    ObMockDirectReadIterator sstable_iter;
    prepare_scan_param(*cg_read_info, trans_version_range, store_ctx, iter_param, context);
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param, context, range, scanner));
    ASSERT_NE(nullptr, scanner);
    ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, *cg_read_info));

    ObMockIterator res_iter;
    res_iter.reset();
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result[i]));
    bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, false/*cmp multi version row flag*/);
    ASSERT_TRUE(is_equal);
    scanner->~ObStoreRowIterator();
  }
}

TEST_P(TestCOMerge, test_merge_range_with_empty)
{
  ObCOMergeTestType type = GetParam();
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  const char *co_table_data[1];
  co_table_data[0]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "0          -8       0        8        EXIST   \n"
      "1          -8       0      NULL        EXIST  \n"
      "3          -8       0        3        EXIST   \n"
      "4          -8       0        2        EXIST   \n"
      "6          -8       0        3        EXIST   \n"
      "7          -8       0        3        EXIST   \n"
      "9          -8       0        3        EXIST   \n"
      "12         -8       0        3        EXIST   \n";

  const char *micro_data1[1];
  micro_data1[0] =
      "bigint     bigint   bigint   bigint  dml           flag    multi_version_row_flag\n"
      "0          -13       0        9         T_DML_UPDATE  EXIST   CLF\n"
      "1          -11      0        NOP       T_DML_INSERT  EXIST   CLF\n"
      "5          -10       0        NOP       T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 1;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);

  //prepare table schema
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version);
  ObArray<ObColDesc> col_ids;
  ASSERT_EQ(OB_SUCCESS, get_col_ids(table_schema_, col_ids));
  ASSERT_EQ(2, col_ids.count());
  add_all_and_each_column_group();
  init_tablet();

  // create co sstable
  ObMockIterator data_iter;
  data_iter.reset();
  OK(data_iter.from(co_table_data[0]));
  ObTableHandleV2 co_table_handle;
  const int64_t micro_row_count[4] = {20, 2, 2, 1};
  const int64_t macro_row_count[4] = {30, 4, 4, 3};
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle, scn_range);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle);
  STORAGE_LOG(INFO, "finish prepare co sstable", KPC(co_table_handle.get_table()));

  ObTableHandleV2 handle1;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1", KPC(handle1.get_table()));


  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;

  //prepare merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
	merge_context.array_count_ = 3;
  alloc_merge_infos(merge_context);
  OK(merge_context.prepare_index_builder(0, 3));

  //prepare merge_range
  ObDatumRowkey start_key, end_key;
  void *buf = allocator_.alloc(sizeof(ObStorageDatum) * 2);
  ObStorageDatum *datums = new (buf) ObStorageDatum[2]();
  datums[0].set_int(10);
  datums[1].set_int(11);
  start_key.datums_ = datums;
  start_key.datum_cnt_ = 1;
  end_key.datums_ = &datums[1];
  end_key.datum_cnt_ = 1;
  ObDatumRange merge_range;
  merge_range.reset();
  merge_range.set_start_key(start_key);
  merge_range.set_end_key(end_key);
  merge_range.set_left_closed();
  merge_range.set_right_closed();


  merge_context.parallel_merge_ctx_.range_array_.reset();
  OK(merge_context.parallel_merge_ctx_.range_array_.push_back(merge_range));
  mix_type_test(type, merge_context, 0, 0, 3);
  OK(create_cg_sstables(merge_context, 0, 3));
  EXPECT_EQ(1, merge_context.merged_cg_tables_handle_.get_count());

  for (int64_t i = 0; i < 3; i++) {
    ObDatumRange range;
    range.set_whole_range();
    trans_version_range.base_version_ = 1;
    trans_version_range.multi_version_start_ = 1;
    trans_version_range.snapshot_version_ = INT64_MAX;

    ObTableIterParam iter_param;
    ObTableAccessContext context;
    const ObITableReadInfo *cg_read_info = nullptr;
    ObStoreCtx store_ctx;

    ObSSTable *merged_sstable = static_cast<ObSSTable *>(merge_context.merged_cg_tables_handle_.get_table(i));
    if (NULL != merged_sstable) {
      EXPECT_EQ(true, merged_sstable->is_co_sstable());
      EXPECT_EQ(true, static_cast<ObCOSSTableV2 *>(merged_sstable)->is_cgs_empty_co_table());
    }
  }
}

TEST_P(TestCOMerge, test_merge_range_is_whole_range)
{
  ObCOMergeTestType type = GetParam();
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  const char *co_table_data[1];
  co_table_data[0]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "0          -8       0        8        EXIST   \n"
      "1          -8       0      NULL        EXIST  \n"
      "3          -8       0        3        EXIST   \n"
      "4          -8       0        2        EXIST   \n"
      "6          -8       0        3        EXIST   \n"
      "7          -8       0        3        EXIST   \n"
      "9          -8       0        3        EXIST   \n"
      "12         -8       0        3        EXIST   \n";

  const char *micro_data1[1];
  micro_data1[0] =
      "bigint     bigint   bigint   bigint  dml           flag    multi_version_row_flag\n"
      "0          -13       0        9         T_DML_UPDATE  EXIST   CLF\n"
      "1          -11      0        NOP       T_DML_INSERT  EXIST   CLF\n"
      "5          -10       0        NOP       T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 1;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);

  //prepare table schema
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version);
  ObArray<ObColDesc> col_ids;
  ASSERT_EQ(OB_SUCCESS, get_col_ids(table_schema_, col_ids));
  ASSERT_EQ(2, col_ids.count());
  add_all_and_each_column_group();
  init_tablet();

  // create co sstable
  ObMockIterator data_iter;
  data_iter.reset();
  OK(data_iter.from(co_table_data[0]));
  ObTableHandleV2 co_table_handle;
  const int64_t micro_row_count[4] = {20, 2, 2, 1};
  const int64_t macro_row_count[4] = {30, 4, 4, 3};
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle, scn_range);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle);

  ObTableHandleV2 handle1;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");


  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;

  //prepare merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
	merge_context.array_count_ = 3;
  alloc_merge_infos(merge_context);
  OK(merge_context.prepare_index_builder(0, 3));

  //prepare merge_range
  ObDatumRange merge_range;
  merge_range.reset();
  merge_range.set_whole_range();


  merge_context.parallel_merge_ctx_.range_array_.reset();
  OK(merge_context.parallel_merge_ctx_.range_array_.push_back(merge_range));
  mix_type_test(type, merge_context, 0, 0, 3);
  OK(create_cg_sstables(merge_context, 0, 3));
  ASSERT_EQ(3, merge_context.merged_cg_tables_handle_.get_count());

  const char *result[3];
  result[0] =
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "0          -13       0        9        EXIST   \n"
      "1          -11       0      NULL        EXIST  \n"
      "3          -8       0        3        EXIST   \n"
      "4          -8       0        2        EXIST   \n"
      "5          -10       0        NULL        EXIST   \n"
      "6          -8       0        3        EXIST   \n"
      "7          -8       0        3        EXIST   \n"
      "9          -8       0        3        EXIST   \n"
      "12         -8       0        3        EXIST   \n";
  result[1] =
      "bigint    flag    multi_version_row_flag\n"
      "0         EXIST   \n"
      "1         EXIST   \n"
      "3         EXIST   \n"
      "4         EXIST   \n"
      "5         EXIST   \n"
      "6         EXIST   \n"
      "7         EXIST   \n"
      "9         EXIST   \n"
      "12         EXIST   \n";
  result[2] =
      "bigint    flag    multi_version_row_flag\n"
      "9         EXIST   \n"
      "NULL         EXIST   \n"
      "3         EXIST   \n"
      "2         EXIST   \n"
      "NULL      EXIST   \n"
      "3         EXIST   \n"
      "3         EXIST   \n"
      "3         EXIST   \n"
      "3         EXIST   \n";

  init_co_sstable(merge_context.merged_cg_tables_handle_, 3);
  for (int64_t i = 0; i < 3; i++) {
    ObDatumRange range;
    range.set_whole_range();
    trans_version_range.base_version_ = 1;
    trans_version_range.multi_version_start_ = 1;
    trans_version_range.snapshot_version_ = INT64_MAX;

    ObTableIterParam iter_param;
    ObTableAccessContext context;
    const ObITableReadInfo *cg_read_info = nullptr;
    ObStoreCtx store_ctx;

    ObSSTable *merged_sstable = static_cast<ObSSTable *>(merge_context.merged_cg_tables_handle_.get_table(i));
    ASSERT_NE(nullptr, merged_sstable);
    if (i > 0) {
      get_cg_read_info(col_ids.at(i - 1), cg_read_info);
    } else {
      cg_read_info = &full_read_info_;
    }

    ObStoreRowIterator *scanner = nullptr;
    ObMockDirectReadIterator sstable_iter;
    prepare_scan_param(*cg_read_info, trans_version_range, store_ctx, iter_param, context);
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param, context, range, scanner));
    ASSERT_NE(nullptr, scanner);
    ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, *cg_read_info));

    ObMockIterator res_iter;
    res_iter.reset();
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result[i]));
    bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, false/*cmp multi version row flag*/);
    ASSERT_TRUE(is_equal);
    scanner->~ObStoreRowIterator();
  }
}

TEST_P(TestCOMerge, test_merge_range_with_beyond_range)
{
  ObCOMergeTestType type = GetParam();
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  const char *co_table_data[1];
  co_table_data[0]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "0          -8       0        8        EXIST   \n"
      "1          -8       0      NULL        EXIST  \n"
      "3          -8       0        3        EXIST   \n"
      "4          -8       0        2        EXIST   \n"
      "6          -8       0        3        EXIST   \n"
      "7          -8       0        3        EXIST   \n"
      "11          -8       0        3        EXIST   \n"
      "12         -8       0        3        EXIST   \n";

  const char *micro_data1[1];
  micro_data1[0] =
      "bigint     bigint   bigint   bigint  dml           flag    multi_version_row_flag\n"
      "0          -13       0        9         T_DML_UPDATE  EXIST   CLF\n"
      "1          -11      0        NOP       T_DML_INSERT  EXIST   CLF\n"
      "5          -10       0        NOP       T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 1;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);

  //prepare table schema
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version);
  ObArray<ObColDesc> col_ids;
  ASSERT_EQ(OB_SUCCESS, get_col_ids(table_schema_, col_ids));
  ASSERT_EQ(2, col_ids.count());
  add_all_and_each_column_group();
  init_tablet();

  // create co sstable
  ObMockIterator data_iter;
  data_iter.reset();
  OK(data_iter.from(co_table_data[0]));
  ObTableHandleV2 co_table_handle;
  const int64_t micro_row_count[4] = {2, 2, 2, 1};
  const int64_t macro_row_count[4] = {30, 4, 4, 3};
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle, scn_range);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle);

  ObTableHandleV2 handle1;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");


  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;

  //prepare merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
	merge_context.array_count_ = 3;
  alloc_merge_infos(merge_context);
  OK(merge_context.prepare_index_builder(0, 3));

  //prepare merge_range
  ObDatumRowkey start_key, end_key;
  void *buf = allocator_.alloc(sizeof(ObStorageDatum) * 2);
  ObStorageDatum *datums = new (buf) ObStorageDatum[2]();
  datums[0].set_int(8);
  datums[1].set_int(9);
  start_key.datums_ = datums;
  start_key.datum_cnt_ = 1;
  end_key.datums_ = &datums[1];
  end_key.datum_cnt_ = 1;
  ObDatumRange merge_range;
  merge_range.reset();
  merge_range.set_start_key(start_key);
  merge_range.set_end_key(end_key);
  merge_range.set_left_closed();
  merge_range.set_right_closed();


  merge_context.parallel_merge_ctx_.range_array_.reset();
  OK(merge_context.parallel_merge_ctx_.range_array_.push_back(merge_range));

  mix_type_test(type, merge_context, 0, 0, 3);
  OK(create_cg_sstables(merge_context, 0, 3));
  EXPECT_EQ(1, merge_context.merged_cg_tables_handle_.get_count());

  for (int64_t i = 0; i < 3; i++) {
    ObDatumRange range;
    range.set_whole_range();
    trans_version_range.base_version_ = 1;
    trans_version_range.multi_version_start_ = 1;
    trans_version_range.snapshot_version_ = INT64_MAX;

    ObTableIterParam iter_param;
    ObTableAccessContext context;
    const ObITableReadInfo *cg_read_info = nullptr;
    ObStoreCtx store_ctx;

    ObSSTable *merged_sstable = static_cast<ObSSTable *>(merge_context.merged_cg_tables_handle_.get_table(i));
    if (NULL != merged_sstable) {
      EXPECT_EQ(true, merged_sstable->is_co_sstable());
      EXPECT_EQ(true, static_cast<ObCOSSTableV2 *>(merged_sstable)->is_cgs_empty_co_table());
    }
  }

}

TEST_P(TestCOMerge, test_rebuild_sstable)
{
  ObCOMergeTestType type = GetParam();
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  const char *co_table_data[1];
  co_table_data[0]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "0          -8       0        8        EXIST   \n"
      "1          -8       0      NULL        EXIST  \n"
      "3          -8       0        3        EXIST   \n"
      "4          -8       0        2        EXIST   \n"
      "6          -8       0        3        EXIST   \n"
      "7          -8       0        3        EXIST   \n"
      "9          -8       0        3        EXIST   \n"
      "12         -8       0        3        EXIST   \n";

  const char *micro_data1[1];
  micro_data1[0] =
      "bigint     bigint   bigint   bigint  dml           flag    multi_version_row_flag\n"
      "0          -13       0        9         T_DML_UPDATE  EXIST   CLF\n"
      "1          -11      0        NOP       T_DML_INSERT  EXIST   CLF\n"
      "5          -10       0        NOP       T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 1;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);

  //prepare table schema
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version);
  ObArray<ObColDesc> col_ids;
  ASSERT_EQ(OB_SUCCESS, get_col_ids(table_schema_, col_ids));
  ASSERT_EQ(2, col_ids.count());
  add_all_and_each_column_group();
  init_tablet();

  // create co sstable
  ObMockIterator data_iter;
  data_iter.reset();
  OK(data_iter.from(co_table_data[0]));
  ObTableHandleV2 co_table_handle;
  const int64_t micro_row_count[4] = {20, 2, 2, 1};
  const int64_t macro_row_count[4] = {30, 4, 4, 3};
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle, scn_range);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle);

  ObTableHandleV2 handle1;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");


  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;

  //prepare merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
  merge_context.static_desc_.concurrent_cnt_ = 2;
	merge_context.array_count_ = 3;
  alloc_merge_infos(merge_context);
  OK(merge_context.prepare_index_builder(0, 3));

  //prepare merge_range
  ObDatumRowkey start_key, end_key;
  void *buf = allocator_.alloc(sizeof(ObStorageDatum) * 2);
  ObStorageDatum *datums = new (buf) ObStorageDatum[2]();
  datums[0].set_int(3);
  datums[1].set_int(6);
  start_key.datums_ = datums;
  start_key.datum_cnt_ = 1;
  end_key.datums_ = &datums[1];
  end_key.datum_cnt_ = 1;
  ObDatumRange merge_range, merge_range_1;
  merge_range_1.reset();
  merge_range_1.set_start_key(start_key);
  merge_range_1.set_end_key(end_key);
  merge_range_1.set_left_closed();
  merge_range_1.set_right_open();

  merge_range.reset();
  merge_range.set_start_key(end_key);
  merge_range.end_key_.set_max_rowkey();
  merge_range.set_left_closed();
  merge_range.set_right_open();


  merge_context.parallel_merge_ctx_.range_array_.reset();
  merge_context.parallel_merge_ctx_.parallel_type_ = ObParallelMergeCtx::PARALLEL_MAJOR;
  merge_context.parallel_merge_ctx_.concurrent_cnt_ = 2;
  OK(merge_context.parallel_merge_ctx_.range_array_.push_back(merge_range_1));
  OK(merge_context.parallel_merge_ctx_.range_array_.push_back(merge_range));
  merge_context.static_param_.concurrent_cnt_ = 2;
  mix_type_test(type, merge_context, 0, 0, 3);
  mix_type_test(type, merge_context, 1, 0, 3, false, false);
  merge_context.cg_merge_info_array_[0]->sstable_builder_.data_store_desc_.get_desc().static_desc_->major_working_cluster_version_ = DATA_VERSION_4_3_0_0;
  merge_context.cg_merge_info_array_[1]->sstable_builder_.data_store_desc_.get_desc().static_desc_->major_working_cluster_version_ = DATA_VERSION_4_3_0_0;
  OK(create_cg_sstables(merge_context, 0, 3));
  ASSERT_EQ(3, merge_context.merged_cg_tables_handle_.get_count());

  const char *result[3];
  result[0] =
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "3          -8       0        3        EXIST   \n"
      "4          -8       0        2        EXIST   \n"
      "5          -10       0        NULL        EXIST   \n"
      "6          -8       0        3        EXIST   \n"
      "7          -8       0        3        EXIST   \n"
      "9          -8       0        3        EXIST   \n"
      "12         -8       0        3        EXIST   \n";
  result[1] =
      "bigint    flag    multi_version_row_flag\n"
      "3         EXIST   \n"
      "4         EXIST   \n"
      "5         EXIST   \n"
      "6         EXIST   \n"
      "7         EXIST   \n"
      "9         EXIST   \n"
      "12         EXIST   \n";
  result[2] =
      "bigint    flag    multi_version_row_flag\n"
      "3         EXIST   \n"
      "2         EXIST   \n"
      "NULL      EXIST   \n"
      "3         EXIST   \n"
      "3         EXIST   \n"
      "3         EXIST   \n"
      "3         EXIST   \n";

  init_co_sstable(merge_context.merged_cg_tables_handle_, 3);
  for (int64_t i = 0; i < 3; i++) {
    ObDatumRange range;
    range.set_whole_range();
    trans_version_range.base_version_ = 1;
    trans_version_range.multi_version_start_ = 1;
    trans_version_range.snapshot_version_ = INT64_MAX;

    ObTableIterParam iter_param;
    ObTableAccessContext context;
    const ObITableReadInfo *cg_read_info = nullptr;
    ObStoreCtx store_ctx;

    ObSSTable *merged_sstable = static_cast<ObSSTable *>(merge_context.merged_cg_tables_handle_.get_table(i));
    ASSERT_NE(nullptr, merged_sstable);
    if (i > 0) {
      get_cg_read_info(col_ids.at(i - 1), cg_read_info);
    } else {
      cg_read_info = &full_read_info_;
    }

    ObStoreRowIterator *scanner = nullptr;
    ObMockDirectReadIterator sstable_iter;
    prepare_scan_param(*cg_read_info, trans_version_range, store_ctx, iter_param, context);
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param, context, range, scanner));
    ASSERT_NE(nullptr, scanner);
    ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, *cg_read_info));

    ObMockIterator res_iter;
    res_iter.reset();
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result[i]));
    bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, false/*cmp multi version row flag*/);
    ASSERT_TRUE(is_equal);
    scanner->~ObStoreRowIterator();
  }
}

TEST_P(TestCOMerge, test_co_merge_log_row_store)
{
  ObCOMergeTestType type = GetParam();
  if (ObCOMergeTestType::NORMAL != type) {
    return;
  }
  int ret = OB_SUCCESS;
  const int64_t block_size = 90; // overflow
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  const char *micro_data[1];
  micro_data[0] =
      "bigint     bigint   bigint   bigint    dml           flag    multi_version_row_flag\n"
      "94         -15       0        9        T_DML_INSERT  EXIST   CLF\n"
      "102        -14       0        NOP       T_DML_UPDATE  EXIST   CLF\n"
      "104        -15       0        NOP       T_DML_DELETE  DELETE   CLF\n"
      "105        -16       0        10       T_DML_INSERT  EXIST   CLF\n"
      "111        -20       0        NOP       T_DML_INSERT  EXIST   CLF\n"
      "120        -18       0        NOP       T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  add_all_and_each_column_group();
  ObArray<ObColDesc> col_ids;
  ASSERT_EQ(OB_SUCCESS, get_col_ids(table_schema_, col_ids));
  ASSERT_EQ(2, col_ids.count());

  ObTableHandleV2 empty_co_table_handle;
  ObStorageSchema storage_schema;
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_empty_sstable(allocator_, storage_schema,
                                            ObTabletID(tablet_id_), snapshot_version, empty_co_table_handle));
  merge_context.static_param_.tables_handle_.add_table(empty_co_table_handle);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;
  //prepare merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
  merge_context.merge_log_storage_ = 1;
  ASSERT_EQ(OB_SUCCESS, merge_context.prepare_two_stage_ctx());
  merge_context.two_stage_ctx_->set_file_block_size(block_size);
  ASSERT_EQ(OB_SUCCESS, merge_context.init_merge_log_mgr(0/*task_id*/));

  ObCOMergeLogFileMgr *mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, merge_context.get_merge_log_mgr(0, mgr));
  ASSERT_EQ(1, mgr->get_row_file_count());

  ObCOMergeLogFileWriter writer;
  ObCOMergeLogFileReader reader(allocator_);
  ObMergeLog merge_log;
  ObMergeLog replay_merge_log;
  ObMergeLog res_merge_log;
  ObMockIterator data_iter;
  data_iter.reset();
  ASSERT_EQ(OB_SUCCESS, data_iter.from_for_datum(micro_data[0]));
  const blocksstable::ObDatumRow *data_row = nullptr;
  const blocksstable::ObDatumRow *res_row = nullptr;
  replay_merge_log.op_ = ObMergeLog::REPLAY;
  replay_merge_log.major_idx_ = 1;
  replay_merge_log.row_id_ = 10;

  const common::ObIArray<ObStorageColumnGroupSchema> &cg_array = merge_context.get_schema()->get_column_groups();
  // init merge log writer
  ASSERT_EQ(OB_SUCCESS, writer.init(allocator_, merge_context, 0/*task_id*/));
  // write merge logs
  merge_log = replay_merge_log;
  // write a REPLAY log without row
  ASSERT_EQ(OB_SUCCESS, writer.write_merge_log(merge_log, nullptr/*no used*/));
  // write INSERT logs with full row
  merge_log.op_ = ObMergeLog::INSERT;
  while (OB_SUCCESS == data_iter.get_next_row(data_row)) {
    if (data_row) {
      merge_log.major_idx_++;
      merge_log.row_id_++;
      ASSERT_EQ(OB_SUCCESS, writer.write_merge_log(merge_log, data_row));
    } else {
      break;
    }
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(4, mgr->get_block_count());
  // init merge log reader after write finish
  ASSERT_EQ(OB_SUCCESS, reader.init(merge_context, 0/*task_id*/, 0/*cg_idx*/));
  // read merge logs and check
  data_iter.reset();
  ASSERT_EQ(OB_SUCCESS, data_iter.from_for_datum(micro_data[0]));
  // read first merge log which is REPLAY log
  merge_log = replay_merge_log;
  ASSERT_EQ(OB_SUCCESS, reader.get_next_log(res_merge_log, res_row));
  ASSERT_EQ(merge_log, res_merge_log);
  ASSERT_EQ(nullptr, res_row);

  // read other logs with row
  merge_log.op_ = ObMergeLog::INSERT;
  bool is_equal = true;
  int ret1 = OB_SUCCESS;
  int ret2 = OB_SUCCESS;

  while (is_equal) {
    ret1 = reader.get_next_log(res_merge_log, res_row);
    ret2 = data_iter.get_next_row(data_row);
    merge_log.major_idx_++;
    merge_log.row_id_++;
    if (ret1 == ret2) {
      if (OB_SUCCESS == ret1 && res_row && data_row) {
        is_equal = ObMockIterator::equals(*res_row, *data_row) && merge_log == res_merge_log;
      } else {
        ASSERT_EQ(OB_ITER_END, ret1);
        break;
      }
    } else {
      is_equal = false;
    }
  }
  ASSERT_EQ(true, is_equal);
  writer.reset();
  reader.reset();
}

TEST_P(TestCOMerge, test_co_merge_log_column_store)
{
  ObCOMergeTestType type = GetParam();
  if (ObCOMergeTestType::NORMAL != type) {
    return;
  }
  int ret = OB_SUCCESS;
  const int64_t block_size = 128;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  const char *micro_data[1];
  micro_data[0] =
      "bigint     bigint   bigint   bigint    dml           flag    multi_version_row_flag\n"
      "94         -15       0        9        T_DML_INSERT  EXIST   CLF\n"
      "102        -14       0        NOP       T_DML_UPDATE  EXIST   CLF\n"
      "104        -15       0        NOP       T_DML_DELETE  DELETE   CLF\n"
      "105        -16       0        10       T_DML_INSERT  EXIST   CLF\n"
      "111        -20       0        NOP       T_DML_INSERT  EXIST   CLF\n"
      "120        -18       0        NOP       T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 1;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  add_all_and_each_column_group();
  ObArray<ObColDesc> col_ids;
  ASSERT_EQ(OB_SUCCESS, get_col_ids(table_schema_, col_ids));
  ASSERT_EQ(2, col_ids.count());

  ObTableHandleV2 empty_co_table_handle;
  ObStorageSchema storage_schema;
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_empty_sstable(allocator_, storage_schema,
                                            ObTabletID(tablet_id_), snapshot_version, empty_co_table_handle));
  merge_context.static_param_.tables_handle_.add_table(empty_co_table_handle);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;
  //prepare merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
  merge_context.merge_log_storage_ = 2;
  ASSERT_EQ(OB_SUCCESS, merge_context.prepare_two_stage_ctx());
  merge_context.two_stage_ctx_->set_file_block_size(block_size);
  ASSERT_EQ(OB_SUCCESS, merge_context.init_merge_log_mgr(0/*task_id*/));

  ObCOMergeLogFileMgr *mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, merge_context.get_merge_log_mgr(0, mgr));
  ASSERT_EQ(3, mgr->get_row_file_count());

  ObCOMergeLogFileWriter writer;
  ObCOMergeLogFileReader reader[3] =
    {ObCOMergeLogFileReader(allocator_), ObCOMergeLogFileReader(allocator_), ObCOMergeLogFileReader(allocator_)};
  ObMergeLog merge_log;
  ObMergeLog replay_merge_log;
  ObMergeLog res_merge_log;
  ObMockIterator data_iter;
  data_iter.reset();
  ASSERT_EQ(OB_SUCCESS, data_iter.from_for_datum(micro_data[0]));
  const blocksstable::ObDatumRow *data_row = nullptr;
  const blocksstable::ObDatumRow *res_row = nullptr;
  replay_merge_log.op_ = ObMergeLog::REPLAY;
  replay_merge_log.major_idx_ = 1;
  replay_merge_log.row_id_ = 10;

  const common::ObIArray<ObStorageColumnGroupSchema> &cg_array = merge_context.get_schema()->get_column_groups();
  ASSERT_EQ(3, cg_array.count());
  // init merge log writer
  ASSERT_EQ(OB_SUCCESS, writer.init(allocator_, merge_context, 0/*task_id*/));
  // write merge logs
  merge_log = replay_merge_log;
  // write a REPLAY log without row
  ASSERT_EQ(OB_SUCCESS, writer.write_merge_log(merge_log, nullptr/*no used*/));
  // write INSERT logs with row
  merge_log.op_ = ObMergeLog::INSERT;
  while (OB_SUCCESS == data_iter.get_next_row(data_row)) {
    if (data_row) {
      merge_log.major_idx_++;
      merge_log.row_id_++;
      ASSERT_EQ(OB_SUCCESS, writer.write_merge_log(merge_log, data_row));
    } else {
      break;
    }
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(3, mgr->get_block_count());
  // init merge log reader after write finish
  ASSERT_EQ(OB_SUCCESS, reader[0].init(merge_context, 0/*task_id*/, 0));
  ASSERT_EQ(OB_SUCCESS, reader[1].init(merge_context, 0/*task_id*/, 1));
  ASSERT_EQ(OB_SUCCESS, reader[2].init(merge_context, 0/*task_id*/, 2));
  // read merge logs and check
  data_iter.reset();
  ASSERT_EQ(OB_SUCCESS, data_iter.from_for_datum(micro_data[0]));
  // read first merge log which is REPLAY log
  merge_log = replay_merge_log;
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, reader[i].get_next_log(res_merge_log, res_row));
    ASSERT_EQ(merge_log, res_merge_log);
    ASSERT_EQ(nullptr, res_row);
  }

  // read other logs with row
  bool is_equal = true;
  int ret1 = OB_SUCCESS;
  int ret2 = OB_SUCCESS;
  for (int64_t i = 0; is_equal && i < 3; ++i) {
    data_iter.reset();
    ASSERT_EQ(OB_SUCCESS, data_iter.from_for_datum(micro_data[0]));
    merge_log = replay_merge_log;
    merge_log.op_ = ObMergeLog::INSERT;
    const ObStorageColumnGroupSchema &cg_schema = cg_array.at(i);
    ObCOMergeProjector projector;
    ObCOMergeProjector *row_project = nullptr;
    if (!cg_schema.is_all_column_group()) {
      OK(projector.init(cg_schema, allocator_));
      row_project = &projector;
    }
    while (is_equal) {
      ret1 = reader[i].get_next_log(res_merge_log, res_row);
      ret2 = data_iter.get_next_row(data_row);
      merge_log.major_idx_++;
      merge_log.row_id_++;
      if (ret1 == ret2) {
        if (OB_SUCCESS == ret1 && data_row && res_row) {
          if (nullptr != row_project) {
            row_project->project(*data_row);
          }
          const ObDatumRow &datum_row = row_project == nullptr ? *data_row : row_project->get_project_row();
          is_equal = ObMockIterator::equals(*res_row, datum_row) && merge_log == res_merge_log;
        } else {
          ASSERT_EQ(OB_ITER_END, ret1);
          break;
        }
      } else {
        is_equal = false;
      }
    }
  }
  ASSERT_EQ(true, is_equal);
  writer.reset();
  reader[0].reset();
  reader[1].reset();
  reader[2].reset();
}

TEST_P(TestCOMerge, test_multi_merge_with_empty_base)
{
  ObCOMergeTestType type = GetParam();
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  const char *co_table_data[1];
  co_table_data[0]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "0          -8       0        8        EXIST   \n"
      "1          -8       0      NULL        EXIST  \n"
      "3          -8       0        3        EXIST   \n"
      "4          -8       0        2        EXIST   \n"
      "6          -8       0        3        EXIST   \n"
      "7          -8       0        3        EXIST   \n"
      "9          -8       0        3        EXIST   \n"
      "12         -8       0        3        EXIST   \n";


  const char *micro_data1[1];
  micro_data1[0] =
      "bigint     bigint   bigint   bigint  dml           flag    multi_version_row_flag\n"
      "0          -13       0        9         T_DML_UPDATE  EXIST   CLF\n"
      "1          -12      0        NOP       T_DML_INSERT  EXIST   CLF\n"
      "5          -11       0        NOP       T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 1;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version);
  add_all_and_each_column_group();
  ObArray<ObColDesc> col_ids;
  ASSERT_EQ(OB_SUCCESS, get_col_ids(table_schema_, col_ids));
  ASSERT_EQ(2, col_ids.count());

  ObTableHandleV2 empty_co_table_handle;

  ObStorageSchema storage_schema;
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_empty_sstable(allocator_, storage_schema,
                                            ObTabletID(tablet_id_), snapshot_version, empty_co_table_handle));

  merge_context.static_param_.tables_handle_.add_table(empty_co_table_handle);

  // create co sstable
  ObMockIterator data_iter;
  data_iter.reset();
  OK(data_iter.from(co_table_data[0]));
  ObTableHandleV2 co_table_handle;
  const int64_t micro_row_count[3] = {2, 2, 1};
  const int64_t macro_row_count[3] = {4, 4, 3};
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle, scn_range);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle);

  ObTableHandleV2 handle1;
  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint    bigint   bigint   bigint  dml          flag    multi_version_row_flag\n"
      "0        -21       0        10      T_DML_UPDATE EXIST   LF\n"
      "2        -22       0        12      T_DML_UPDATE EXIST   LF\n"
      "3        -25       0        13      T_DML_UPDATE EXIST   LF\n";

  snapshot_version = 30;
  table_key_.scn_range_.start_scn_.convert_for_tx(20);
  table_key_.scn_range_.end_scn_.convert_for_tx(30);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;

  //prepare merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
  merge_context.array_count_ = 3;
  alloc_merge_infos(merge_context);
  OK(merge_context.prepare_index_builder(0, 3));

  mix_type_test(type, merge_context, 0, 0, 3);

  // create all cg sstable
  OK(create_cg_sstables(merge_context, 0, 3));
  ASSERT_EQ(3, merge_context.merged_cg_tables_handle_.get_count());

  const char *result[3];
  result[0] =
      "bigint    bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "0          -21       0        10    EXIST   \n"
      "1          -12       0      NULL    EXIST   \n"
      "2          -22       0        12    EXIST   \n"
      "3          -25       0        13    EXIST   \n"
      "4          -8       0        2        EXIST   \n"
      "5          -11       0       NULL     EXIST   \n"
      "6          -8       0        3        EXIST   \n"
      "7          -8       0        3        EXIST   \n"
      "9          -8       0        3        EXIST   \n"
      "12         -8       0        3        EXIST   \n";

  result[1] =
      "bigint    flag    multi_version_row_flag\n"
      "0         EXIST   \n"
      "1         EXIST   \n"
      "2         EXIST   \n"
      "3         EXIST   \n"
      "4         EXIST   \n"
      "5         EXIST   \n"
      "6         EXIST   \n"
      "7         EXIST   \n"
      "9         EXIST   \n"
      "12         EXIST   \n";
  result[2] =
      "bigint    flag    multi_version_row_flag\n"
      "10         EXIST   \n"
      "NULL         EXIST   \n"
      "12         EXIST   \n"
      "13         EXIST   \n"
      "2         EXIST   \n"
      "NULL      EXIST   \n"
      "3         EXIST   \n"
      "3         EXIST   \n"
      "3         EXIST   \n"
      "3         EXIST   \n";

  init_co_sstable(merge_context.merged_cg_tables_handle_, 3);
  for (int64_t i = 0; i < 3; i++) {
    ObDatumRange range;
    range.set_whole_range();
    trans_version_range.base_version_ = 1;
    trans_version_range.multi_version_start_ = 1;
    trans_version_range.snapshot_version_ = INT64_MAX;

    ObTableIterParam iter_param;
    ObTableAccessContext context;
    const ObITableReadInfo *cg_read_info = nullptr;
    ObStoreCtx store_ctx;

    ObSSTable *merged_sstable = static_cast<ObSSTable *>(merge_context.merged_cg_tables_handle_.get_table(i));
    ASSERT_NE(nullptr, merged_sstable);
    if (i > 0) {
      get_cg_read_info(col_ids.at(i - 1), cg_read_info);
    } else {
      cg_read_info = &full_read_info_;
    }

    ObStoreRowIterator *scanner = nullptr;
    ObMockDirectReadIterator sstable_iter;
    prepare_scan_param(*cg_read_info, trans_version_range, store_ctx, iter_param, context);
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param, context, range, scanner));
    ASSERT_NE(nullptr, scanner);
    ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, *cg_read_info));

    ObMockIterator res_iter;
    res_iter.reset();
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result[i]));
    bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, false/*cmp multi version row flag*/);
    ASSERT_TRUE(is_equal);
    scanner->~ObStoreRowIterator();
  }
}

TEST_P(TestCOMerge, test_multi_merge_with_empty_inc_major)
{
  ObCOMergeTestType type = GetParam();
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  const char *co_table_data[1];
  co_table_data[0]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "0          -8       0        8        EXIST   \n"
      "1          -8       0      NULL        EXIST  \n"
      "3          -8       0        3        EXIST   \n"
      "4          -8       0        2        EXIST   \n"
      "6          -8       0        3        EXIST   \n"
      "7          -8       0        3        EXIST   \n"
      "9          -8       0        3        EXIST   \n"
      "12         -8       0        3        EXIST   \n";


  const char *micro_data1[1];
  micro_data1[0] =
      "bigint     bigint   bigint   bigint  dml           flag    multi_version_row_flag\n"
      "0          -13       0        9         T_DML_UPDATE  EXIST   CLF\n"
      "1          -12      0        NOP       T_DML_INSERT  EXIST   CLF\n"
      "5          -11       0        NOP       T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 1;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version);
  add_all_and_each_column_group();
  ObArray<ObColDesc> col_ids;
  ASSERT_EQ(OB_SUCCESS, get_col_ids(table_schema_, col_ids));
  ASSERT_EQ(2, col_ids.count());

  ObTableHandleV2 empty_co_table_handle;

  ObStorageSchema storage_schema;
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_empty_sstable(allocator_, storage_schema,
                                            ObTabletID(tablet_id_), snapshot_version, empty_co_table_handle));

  // create co sstable
  ObMockIterator data_iter;
  data_iter.reset();
  OK(data_iter.from(co_table_data[0]));
  ObTableHandleV2 co_table_handle;
  const int64_t micro_row_count[3] = {2, 2, 1};
  const int64_t macro_row_count[3] = {4, 4, 3};
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle, scn_range);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle); // major
  merge_context.static_param_.tables_handle_.add_table(empty_co_table_handle); // empty inc major

  ObTableHandleV2 handle1;
  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint    bigint   bigint   bigint  dml          flag    multi_version_row_flag\n"
      "0        -21       0        10      T_DML_UPDATE EXIST   LF\n"
      "2        -22       0        12      T_DML_UPDATE EXIST   LF\n"
      "3        -25       0        13      T_DML_UPDATE EXIST   LF\n";

  snapshot_version = 30;
  table_key_.scn_range_.start_scn_.convert_for_tx(20);
  table_key_.scn_range_.end_scn_.convert_for_tx(30);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;

  //prepare merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
  merge_context.array_count_ = 3;
  alloc_merge_infos(merge_context);
  OK(merge_context.prepare_index_builder(0, 3));

  mix_type_test(type, merge_context, 0, 0, 3);

  // create all cg sstable
  OK(create_cg_sstables(merge_context, 0, 3));
  ASSERT_EQ(3, merge_context.merged_cg_tables_handle_.get_count());

  const char *result[3];
  result[0] =
      "bigint    bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "0          -21       0        10    EXIST   \n"
      "1          -12       0      NULL    EXIST   \n"
      "2          -22       0        12    EXIST   \n"
      "3          -25       0        13    EXIST   \n"
      "4          -8       0        2        EXIST   \n"
      "5          -11       0       NULL     EXIST   \n"
      "6          -8       0        3        EXIST   \n"
      "7          -8       0        3        EXIST   \n"
      "9          -8       0        3        EXIST   \n"
      "12         -8       0        3        EXIST   \n";

  result[1] =
      "bigint    flag    multi_version_row_flag\n"
      "0         EXIST   \n"
      "1         EXIST   \n"
      "2         EXIST   \n"
      "3         EXIST   \n"
      "4         EXIST   \n"
      "5         EXIST   \n"
      "6         EXIST   \n"
      "7         EXIST   \n"
      "9         EXIST   \n"
      "12         EXIST   \n";
  result[2] =
      "bigint    flag    multi_version_row_flag\n"
      "10         EXIST   \n"
      "NULL         EXIST   \n"
      "12         EXIST   \n"
      "13         EXIST   \n"
      "2         EXIST   \n"
      "NULL      EXIST   \n"
      "3         EXIST   \n"
      "3         EXIST   \n"
      "3         EXIST   \n"
      "3         EXIST   \n";

  init_co_sstable(merge_context.merged_cg_tables_handle_, 3);
  for (int64_t i = 0; i < 3; i++) {
    ObDatumRange range;
    range.set_whole_range();
    trans_version_range.base_version_ = 1;
    trans_version_range.multi_version_start_ = 1;
    trans_version_range.snapshot_version_ = INT64_MAX;

    ObTableIterParam iter_param;
    ObTableAccessContext context;
    const ObITableReadInfo *cg_read_info = nullptr;
    ObStoreCtx store_ctx;

    ObSSTable *merged_sstable = static_cast<ObSSTable *>(merge_context.merged_cg_tables_handle_.get_table(i));
    ASSERT_NE(nullptr, merged_sstable);
    if (i > 0) {
      get_cg_read_info(col_ids.at(i - 1), cg_read_info);
    } else {
      cg_read_info = &full_read_info_;
    }

    ObStoreRowIterator *scanner = nullptr;
    ObMockDirectReadIterator sstable_iter;
    prepare_scan_param(*cg_read_info, trans_version_range, store_ctx, iter_param, context);
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param, context, range, scanner));
    ASSERT_NE(nullptr, scanner);
    ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, *cg_read_info));

    ObMockIterator res_iter;
    res_iter.reset();
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result[i]));
    bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, false/*cmp multi version row flag*/);
    ASSERT_TRUE(is_equal);
    scanner->~ObStoreRowIterator();
  }
}

TEST_P(TestCOMerge, test_multi_merge_with_empty_range)
{
  ObCOMergeTestType type = GetParam();
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  const char *co_table_data[2];
  co_table_data[0]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "0          -8       0        8        EXIST   \n"
      "1          -8       0      NULL       EXIST   \n"
      "7          -8       0        3        EXIST   \n"
      "9          -8       0        3        EXIST   \n"
      "12         -8       0        3        EXIST   \n";

  co_table_data[1]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "0          -8       0        8        EXIST   \n"
      "1          -8       0      NULL       EXIST   \n"
      "2          -8       0        3        EXIST   \n"
      "6          -8       0        3        EXIST   \n"
      "9          -8       0        3        EXIST   \n"
      "12         -8       0        3        EXIST   \n";

  const char *micro_data1[1];
  micro_data1[0] =
      "bigint     bigint   bigint   bigint  dml           flag    multi_version_row_flag\n"
      "3          -13       0        9         T_DML_UPDATE  EXIST   CLF\n"
      "4          -12       0        NOP       T_DML_INSERT  EXIST   CLF\n"
      "5          -11       0        NOP       T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 1;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version);
  add_all_and_each_column_group();
  ObArray<ObColDesc> col_ids;
  ASSERT_EQ(OB_SUCCESS, get_col_ids(table_schema_, col_ids));
  ASSERT_EQ(2, col_ids.count());

  // create co sstable 1
  ObMockIterator data_iter;
  data_iter.reset();
  OK(data_iter.from(co_table_data[0]));
  ObTableHandleV2 co_table_handle1;
  const int64_t micro_row_count[3] = {2, 2, 1};
  const int64_t macro_row_count[3] = {2, 4, 3};
  scn_range.start_scn_.convert_for_tx(0);
  scn_range.end_scn_.convert_for_tx(8);
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle1, scn_range);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle1.get_table())->cs_meta_.column_group_cnt_);

  // create co sstable 1
  data_iter.reset();
  OK(data_iter.from(co_table_data[1]));
  ObTableHandleV2 co_table_handle2;
  scn_range.start_scn_.convert_for_tx(8);
  scn_range.end_scn_.convert_for_tx(10);
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle2, scn_range);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle2.get_table())->cs_meta_.column_group_cnt_);

  merge_context.static_param_.tables_handle_.add_table(co_table_handle1);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle2);

  ObTableHandleV2 handle1;
  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");


  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;

  //prepare merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
  merge_context.array_count_ = 3;
  alloc_merge_infos(merge_context);
  OK(merge_context.prepare_index_builder(0, 3));

  //prepare merge_range
  ObDatumRowkey start_key, end_key;
  void *buf = allocator_.alloc(sizeof(ObStorageDatum) * 2);
  ObStorageDatum *datums = new (buf) ObStorageDatum[2]();
  datums[0].set_int(3);
  datums[1].set_int(6);
  start_key.datums_ = datums;
  start_key.datum_cnt_ = 1;
  end_key.datums_ = &datums[1];
  end_key.datum_cnt_ = 1;
  ObDatumRange merge_range;
  merge_range.reset();
  merge_range.set_start_key(start_key);
  merge_range.set_end_key(end_key);
  merge_range.set_left_closed();
  merge_range.set_right_closed();

  merge_context.parallel_merge_ctx_.range_array_.reset();
  OK(merge_context.parallel_merge_ctx_.range_array_.push_back(merge_range));

  mix_type_test(type, merge_context, 0, 0, 3);

  // create all cg sstable
  OK(create_cg_sstables(merge_context, 0, 3));
  ASSERT_EQ(3, merge_context.merged_cg_tables_handle_.get_count());

  const char *result[3];
  result[0] =
      "bigint    bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "3          -13       0       9      EXIST   \n"
      "4          -12       0       NULL   EXIST   \n"
      "5          -11       0       NULL   EXIST   \n"
      "6          -8        0       3   EXIST   \n";

  result[1] =
      "bigint    flag    multi_version_row_flag\n"
      "3         EXIST   \n"
      "4         EXIST   \n"
      "5         EXIST   \n"
      "6         EXIST   \n";
  result[2] =
      "bigint    flag    multi_version_row_flag\n"
      "9         EXIST   \n"
      "NULL      EXIST   \n"
      "NULL      EXIST   \n"
      "3         EXIST   \n";

  init_co_sstable(merge_context.merged_cg_tables_handle_, 3);
  for (int64_t i = 0; i < 3; i++) {
    ObDatumRange range;
    range.set_whole_range();
    trans_version_range.base_version_ = 1;
    trans_version_range.multi_version_start_ = 1;
    trans_version_range.snapshot_version_ = INT64_MAX;

    ObTableIterParam iter_param;
    ObTableAccessContext context;
    const ObITableReadInfo *cg_read_info = nullptr;
    ObStoreCtx store_ctx;

    ObSSTable *merged_sstable = static_cast<ObSSTable *>(merge_context.merged_cg_tables_handle_.get_table(i));
    ASSERT_NE(nullptr, merged_sstable);
    if (i > 0) {
      get_cg_read_info(col_ids.at(i - 1), cg_read_info);
    } else {
      cg_read_info = &full_read_info_;
    }

    ObStoreRowIterator *scanner = nullptr;
    ObMockDirectReadIterator sstable_iter;
    prepare_scan_param(*cg_read_info, trans_version_range, store_ctx, iter_param, context);
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param, context, range, scanner));
    ASSERT_NE(nullptr, scanner);
    ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, *cg_read_info));

    ObMockIterator res_iter;
    res_iter.reset();
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result[i]));
    bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, false/*cmp multi version row flag*/);
    ASSERT_TRUE(is_equal);
    scanner->~ObStoreRowIterator();
  }
}

// TODO Complicate this use case
TEST_P(TestCOMerge, test_multi_merge_with_three_pure_co)
{
  ObCOMergeTestType type = GetParam();
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  const char *co_table_data1[1];
  co_table_data1[0]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "100          -8       0        12        EXIST   \n"
      "103          -8       0      NULL        EXIST  \n"
      "104          -8       0        3        EXIST   \n"
      "110          -8       0        2        EXIST   \n"
      "120          -8       0        3        EXIST   \n";

  const char *co_table_data2[1];
  co_table_data2[0]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "101          -9       0      NULL        EXIST   \n"
      "102          -9       0      NULL        EXIST  \n"
      "108          -9       0        4        EXIST   \n"
      "109          -9       0        6        EXIST   \n"
      "123          -9       0        8        EXIST   \n";

  const char *co_table_data3[1];
  co_table_data3[0]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "96          -10       0      16        EXIST   \n"
      "99          -10       0      5        EXIST  \n"
      "106         -10       0      -12        EXIST   \n"
      "107         -10       0      23        EXIST   \n"
      "112         -10       0      NULL        EXIST   \n";

  const char *micro_data1[1];
  micro_data1[0] =
      "bigint     bigint   bigint   bigint    dml           flag    multi_version_row_flag\n"
      "94         -15       0        9        T_DML_INSERT  EXIST   CLF\n"
      "102        -14       0        NOP       T_DML_UPDATE  EXIST   CLF\n"
      "104        -15       0        NOP       T_DML_DELETE  DELETE   CLF\n"
      "105        -16       0        10       T_DML_INSERT  EXIST   CLF\n"
      "111        -20       0        NOP       T_DML_INSERT  EXIST   CLF\n"
      "120        -18       0        NOP       T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 1;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version);
  add_rowkey_and_each_column_group();
  ObArray<ObColDesc> col_ids;
  ASSERT_EQ(OB_SUCCESS, get_col_ids(table_schema_, col_ids));
  ASSERT_EQ(2, col_ids.count());

  // create co sstable 1
  ObMockIterator data_iter;
  data_iter.reset();
  OK(data_iter.from(co_table_data1[0]));
  ObTableHandleV2 co_table_handle1;
  const int64_t micro_row_count[3] = {2, 2, 1};
  const int64_t macro_row_count[3] = {2, 4, 3};
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle1, scn_range, false/*is_all_cg_base*/);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle1.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle1);

  // create co sstable 2
  data_iter.reset();
  OK(data_iter.from(co_table_data2[0]));
  ObTableHandleV2 co_table_handle2;
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle2, scn_range, false/*is_all_cg_base*/);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle2.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle2);

  // create co sstable 3
  data_iter.reset();
  OK(data_iter.from(co_table_data3[0]));
  ObTableHandleV2 co_table_handle3;
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle3, scn_range, false/*is_all_cg_base*/);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle3.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle3);

  ObTableHandleV2 handle1;
  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;

  //prepare merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
  merge_context.array_count_ = 3;
  merge_context.static_param_.is_full_merge_ = true;
  alloc_merge_infos(merge_context);
  OK(merge_context.prepare_index_builder(0, 3));

  mix_type_test(type, merge_context, 0, 0, 3);

  // create all cg sstable
  OK(create_cg_sstables(merge_context, 0, 3));
  ASSERT_EQ(3, merge_context.merged_cg_tables_handle_.get_count());

  const char *result[3];
  result[0] =
      "bigint    bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "94          -15       0        9    EXIST   \n"
      "96          -10       0      16    EXIST   \n"
      "99          -10       0        5    EXIST   \n"
      "100          -8       0        12    EXIST   \n"
      "101          -9       0        NULL        EXIST   \n"
      "102          -14       0       NULL     EXIST   \n"
      "103          -8       0        NULL        EXIST   \n"
      "105          -16       0        10        EXIST   \n"
      "106          -10       0        -12        EXIST   \n"
      "107          -10       0        23        EXIST   \n"
      "108          -9       0        4        EXIST   \n"
      "109          -9       0        6        EXIST   \n"
      "110          -8       0        2        EXIST   \n"
      "111          -20       0        NULL        EXIST   \n"
      "112          -10       0        NULL        EXIST   \n"
      "120          -18       0        3        EXIST   \n"
      "123         -9       0        8        EXIST   \n";

  result[1] =
      "bigint    flag    multi_version_row_flag\n"
      "94         EXIST   \n"
      "96         EXIST   \n"
      "99         EXIST   \n"
      "100         EXIST   \n"
      "101         EXIST   \n"
      "102         EXIST   \n"
      "103         EXIST   \n"
      "105         EXIST   \n"
      "106         EXIST   \n"
      "107         EXIST   \n"
      "108         EXIST   \n"
      "109         EXIST   \n"
      "110         EXIST   \n"
      "111         EXIST   \n"
      "112         EXIST   \n"
      "120         EXIST   \n"
      "123         EXIST   \n";
  result[2] =
      "bigint    flag    multi_version_row_flag\n"
      "9         EXIST   \n"
      "16         EXIST   \n"
      "5         EXIST   \n"
      "12         EXIST   \n"
      "NULL         EXIST   \n"
      "NULL         EXIST   \n"
      "NULL         EXIST   \n"
      "10         EXIST   \n"
      "-12         EXIST   \n"
      "23         EXIST   \n"
      "4         EXIST   \n"
      "6         EXIST   \n"
      "2         EXIST   \n"
      "NULL         EXIST   \n"
      "NULL         EXIST   \n"
      "3         EXIST   \n"
      "8         EXIST   \n";

  init_co_sstable(merge_context.merged_cg_tables_handle_, 3);
  ObArray<int32_t> out_col_project;
  for (int64_t i = 0; i < 3; i++) {
    ObDatumRange range;
    range.set_whole_range();
    trans_version_range.base_version_ = 1;
    trans_version_range.multi_version_start_ = 1;
    trans_version_range.snapshot_version_ = INT64_MAX;

    ObTableIterParam iter_param;
    ObTableAccessContext context;
    const ObITableReadInfo *cg_read_info = nullptr;
    ObStoreCtx store_ctx;

    ObSSTable *merged_sstable = static_cast<ObSSTable *>(merge_context.merged_cg_tables_handle_.get_table(i));
    ASSERT_NE(nullptr, merged_sstable);
    if (i > 0) {
      get_cg_read_info(col_ids.at(i - 1), cg_read_info);
    } else {
      ASSERT_EQ(OB_SUCCESS, merge_context.mock_row_store_table_read_info());
      cg_read_info = &merge_context.mocked_row_store_table_read_info_;
    }

    ObStoreRowIterator *scanner = nullptr;
    ObMockDirectReadIterator sstable_iter;
    prepare_scan_param(*cg_read_info, trans_version_range, store_ctx, iter_param, context);
    if (i == 0) {
      construct_out_cols(out_col_project, iter_param);
    }
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param, context, range, scanner));
    ASSERT_NE(nullptr, scanner);

    ObMockIterator res_iter;
    res_iter.reset();

    bool is_equal = true;
    if (i == 0) {
      ASSERT_EQ(OB_SUCCESS, res_iter.from_for_datum(result[i]));
      is_equal = compare_datum_row(&res_iter, scanner);
    } else {
      ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, *cg_read_info));
      ASSERT_EQ(OB_SUCCESS, res_iter.from(result[i]));
      is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, false/*cmp multi version row flag*/);
    }
    ASSERT_TRUE(is_equal);
    scanner->~ObStoreRowIterator();
  }
}

TEST_P(TestCOMerge, test_multi_merge_with_three_major)
{
  ObCOMergeTestType type = GetParam();
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  const char *co_table_data1[1];
  co_table_data1[0]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "100          -8       0        12        EXIST   \n"
      "103          -8       0      NULL        EXIST  \n"
      "104          -8       0        3        EXIST   \n"
      "110          -8       0        2        EXIST   \n"
      "120          -8       0        3        EXIST   \n";

  const char *co_table_data2[1];
  co_table_data2[0]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "101          -9       0      NULL        EXIST   \n"
      "102          -9       0      NULL        EXIST  \n"
      "108          -9       0        4        EXIST   \n"
      "109          -9       0        6        EXIST   \n"
      "123          -9       0        8        EXIST   \n";

  const char *co_table_data3[1];
  co_table_data3[0]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "96          -10       0      16        EXIST   \n"
      "99          -10       0      5        EXIST  \n"
      "106         -10       0      -12        EXIST   \n"
      "107         -10       0      23        EXIST   \n"
      "112         -10       0      NULL        EXIST   \n";

  const char *micro_data1[1];
  micro_data1[0] =
      "bigint     bigint   bigint   bigint    dml           flag    multi_version_row_flag\n"
      "94         -15       0        9        T_DML_INSERT  EXIST   CLF\n"
      "102        -14       0        NOP       T_DML_UPDATE  EXIST   CLF\n"
      "104        -15       0        NOP       T_DML_DELETE  DELETE   CLF\n"
      "105        -16       0        10       T_DML_INSERT  EXIST   CLF\n"
      "111        -20       0        NOP       T_DML_INSERT  EXIST   CLF\n"
      "120        -18       0        NOP       T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 1;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version);
  add_all_and_each_column_group();
  ObArray<ObColDesc> col_ids;
  ASSERT_EQ(OB_SUCCESS, get_col_ids(table_schema_, col_ids));
  ASSERT_EQ(2, col_ids.count());

  // create co sstable 1
  ObMockIterator data_iter;
  data_iter.reset();
  OK(data_iter.from(co_table_data1[0]));
  ObTableHandleV2 co_table_handle1;
  const int64_t micro_row_count[3] = {2, 2, 1};
  const int64_t macro_row_count[3] = {2, 4, 3};
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle1, scn_range);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle1.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle1);

  // create co sstable 2
  data_iter.reset();
  OK(data_iter.from(co_table_data2[0]));
  ObTableHandleV2 co_table_handle2;
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle2, scn_range);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle2.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle2);

  // create co sstable 3
  data_iter.reset();
  OK(data_iter.from(co_table_data3[0]));
  ObTableHandleV2 co_table_handle3;
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle3, scn_range);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle3.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle3);

  ObTableHandleV2 handle1;
  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;

  //prepare merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
  merge_context.array_count_ = 3;
  alloc_merge_infos(merge_context);
  OK(merge_context.prepare_index_builder(0, 3));

  mix_type_test(type, merge_context, 0, 0, 3);

  // create all cg sstable
  OK(create_cg_sstables(merge_context, 0, 3));
  ASSERT_EQ(3, merge_context.merged_cg_tables_handle_.get_count());

  const char *result[3];
  result[0] =
      "bigint    bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "94          -15       0        9    EXIST   \n"
      "96          -10       0      16    EXIST   \n"
      "99          -10       0        5    EXIST   \n"
      "100          -8       0        12    EXIST   \n"
      "101          -9       0        NULL        EXIST   \n"
      "102          -14       0       NULL     EXIST   \n"
      "103          -8       0        NULL        EXIST   \n"
      "105          -16       0        10        EXIST   \n"
      "106          -10       0        -12        EXIST   \n"
      "107          -10       0        23        EXIST   \n"
      "108          -9       0        4        EXIST   \n"
      "109          -9       0        6        EXIST   \n"
      "110          -8       0        2        EXIST   \n"
      "111          -20       0        NULL        EXIST   \n"
      "112          -10       0        NULL        EXIST   \n"
      "120          -18       0        3        EXIST   \n"
      "123         -9       0        8        EXIST   \n";

  result[1] =
      "bigint    flag    multi_version_row_flag\n"
      "94         EXIST   \n"
      "96         EXIST   \n"
      "99         EXIST   \n"
      "100         EXIST   \n"
      "101         EXIST   \n"
      "102         EXIST   \n"
      "103         EXIST   \n"
      "105         EXIST   \n"
      "106         EXIST   \n"
      "107         EXIST   \n"
      "108         EXIST   \n"
      "109         EXIST   \n"
      "110         EXIST   \n"
      "111         EXIST   \n"
      "112         EXIST   \n"
      "120         EXIST   \n"
      "123         EXIST   \n";
  result[2] =
      "bigint    flag    multi_version_row_flag\n"
      "9         EXIST   \n"
      "16         EXIST   \n"
      "5         EXIST   \n"
      "12         EXIST   \n"
      "NULL         EXIST   \n"
      "NULL         EXIST   \n"
      "NULL         EXIST   \n"
      "10         EXIST   \n"
      "-12         EXIST   \n"
      "23         EXIST   \n"
      "4         EXIST   \n"
      "6         EXIST   \n"
      "2         EXIST   \n"
      "NULL         EXIST   \n"
      "NULL         EXIST   \n"
      "3         EXIST   \n"
      "8         EXIST   \n";

  init_co_sstable(merge_context.merged_cg_tables_handle_, 3);
  for (int64_t i = 0; i < 3; i++) {
    ObDatumRange range;
    range.set_whole_range();
    trans_version_range.base_version_ = 1;
    trans_version_range.multi_version_start_ = 1;
    trans_version_range.snapshot_version_ = INT64_MAX;

    ObTableIterParam iter_param;
    ObTableAccessContext context;
    const ObITableReadInfo *cg_read_info = nullptr;
    ObStoreCtx store_ctx;

    ObSSTable *merged_sstable = static_cast<ObSSTable *>(merge_context.merged_cg_tables_handle_.get_table(i));
    ASSERT_NE(nullptr, merged_sstable);
    if (i > 0) {
      get_cg_read_info(col_ids.at(i - 1), cg_read_info);
    } else {
      cg_read_info = &full_read_info_;
    }

    ObStoreRowIterator *scanner = nullptr;
    ObMockDirectReadIterator sstable_iter;
    prepare_scan_param(*cg_read_info, trans_version_range, store_ctx, iter_param, context);
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param, context, range, scanner));
    ASSERT_NE(nullptr, scanner);
    ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, *cg_read_info));

    ObMockIterator res_iter;
    res_iter.reset();
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result[i]));
    bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, false/*cmp multi version row flag*/);
    ASSERT_TRUE(is_equal);
    scanner->~ObStoreRowIterator();
  }
}

TEST_P(TestCOMerge, test_only_row_with_three_major)
{
  ObCOMergeTestType type = GetParam();
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  const char *co_table_data1[1];
  co_table_data1[0]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "100          -8       0        12        EXIST   \n"
      "103          -8       0      NULL        EXIST  \n"
      "104          -8       0        3        EXIST   \n"
      "110          -8       0        2        EXIST   \n"
      "120          -8       0        3        EXIST   \n";

  const char *co_table_data2[1];
  co_table_data2[0]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "101          -9       0      NULL        EXIST   \n"
      "102          -9       0      NULL        EXIST  \n"
      "108          -9       0        4        EXIST   \n"
      "109          -9       0        6        EXIST   \n"
      "123          -9       0        8        EXIST   \n";

  const char *co_table_data3[1];
  co_table_data3[0]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "96          -10       0      16        EXIST   \n"
      "99          -10       0      5        EXIST  \n"
      "106         -10       0      -12        EXIST   \n"
      "107         -10       0      23        EXIST   \n"
      "112         -10       0      NULL        EXIST   \n";

  const char *micro_data1[1];
  micro_data1[0] =
      "bigint     bigint   bigint   bigint    dml           flag    multi_version_row_flag\n"
      "94         -15       0        9        T_DML_INSERT  EXIST   CLF\n"
      "102        -14       0        NOP       T_DML_UPDATE  EXIST   CLF\n"
      "104        -15       0        NOP       T_DML_DELETE  DELETE   CLF\n"
      "105        -16       0        10       T_DML_INSERT  EXIST   CLF\n"
      "111        -20       0        NOP       T_DML_INSERT  EXIST   CLF\n"
      "120        -18       0        NOP       T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 1;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version);
  add_all_and_each_column_group();
  ObArray<ObColDesc> col_ids;
  ASSERT_EQ(OB_SUCCESS, get_col_ids(table_schema_, col_ids));
  ASSERT_EQ(2, col_ids.count());

  // create co sstable 1
  ObMockIterator data_iter;
  data_iter.reset();
  OK(data_iter.from(co_table_data1[0]));
  ObTableHandleV2 co_table_handle1;
  const int64_t micro_row_count[3] = {2, 2, 1};
  const int64_t macro_row_count[3] = {2, 4, 3};
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle1, scn_range);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle1.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle1);

  // create co sstable 2
  data_iter.reset();
  OK(data_iter.from(co_table_data2[0]));
  ObTableHandleV2 co_table_handle2;
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle2, scn_range);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle2.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle2);

  // create co sstable 3
  data_iter.reset();
  OK(data_iter.from(co_table_data3[0]));
  ObTableHandleV2 co_table_handle3;
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle3, scn_range);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle3.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle3);

  ObTableHandleV2 handle1;
  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;

  //prepare merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
  merge_context.array_count_ = 3;
  alloc_merge_infos(merge_context);
  OK(merge_context.prepare_index_builder(0, 3));

  mix_type_test(type, merge_context, 0, 0, 3, true, true, true);

  // create all cg sstable
  OK(create_cg_sstables(merge_context, 0, 3));
  ASSERT_EQ(3, merge_context.merged_cg_tables_handle_.get_count());

  const char *result[3];
  result[0] =
      "bigint    bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "94          -15       0        9    EXIST   \n"
      "96          -10       0      16    EXIST   \n"
      "99          -10       0        5    EXIST   \n"
      "100          -8       0        12    EXIST   \n"
      "101          -9       0        NULL        EXIST   \n"
      "102          -14       0       NULL     EXIST   \n"
      "103          -8       0        NULL        EXIST   \n"
      "105          -16       0        10        EXIST   \n"
      "106          -10       0        -12        EXIST   \n"
      "107          -10       0        23        EXIST   \n"
      "108          -9       0        4        EXIST   \n"
      "109          -9       0        6        EXIST   \n"
      "110          -8       0        2        EXIST   \n"
      "111          -20       0        NULL        EXIST   \n"
      "112          -10       0        NULL        EXIST   \n"
      "120          -18       0        3        EXIST   \n"
      "123         -9       0        8        EXIST   \n";

  result[1] =
      "bigint    flag    multi_version_row_flag\n"
      "94         EXIST   \n"
      "96         EXIST   \n"
      "99         EXIST   \n"
      "100         EXIST   \n"
      "101         EXIST   \n"
      "102         EXIST   \n"
      "103         EXIST   \n"
      "105         EXIST   \n"
      "106         EXIST   \n"
      "107         EXIST   \n"
      "108         EXIST   \n"
      "109         EXIST   \n"
      "110         EXIST   \n"
      "111         EXIST   \n"
      "112         EXIST   \n"
      "120         EXIST   \n"
      "123         EXIST   \n";
  result[2] =
      "bigint    flag    multi_version_row_flag\n"
      "9         EXIST   \n"
      "16         EXIST   \n"
      "5         EXIST   \n"
      "12         EXIST   \n"
      "NULL         EXIST   \n"
      "NULL         EXIST   \n"
      "NULL         EXIST   \n"
      "10         EXIST   \n"
      "-12         EXIST   \n"
      "23         EXIST   \n"
      "4         EXIST   \n"
      "6         EXIST   \n"
      "2         EXIST   \n"
      "NULL         EXIST   \n"
      "NULL         EXIST   \n"
      "3         EXIST   \n"
      "8         EXIST   \n";

  init_co_sstable(merge_context.merged_cg_tables_handle_, 3);
  for (int64_t i = 0; i < 3; i++) {
    ObDatumRange range;
    range.set_whole_range();
    trans_version_range.base_version_ = 1;
    trans_version_range.multi_version_start_ = 1;
    trans_version_range.snapshot_version_ = INT64_MAX;

    ObTableIterParam iter_param;
    ObTableAccessContext context;
    const ObITableReadInfo *cg_read_info = nullptr;
    ObStoreCtx store_ctx;

    ObSSTable *merged_sstable = static_cast<ObSSTable *>(merge_context.merged_cg_tables_handle_.get_table(i));
    ASSERT_NE(nullptr, merged_sstable);
    if (i > 0) {
      get_cg_read_info(col_ids.at(i - 1), cg_read_info);
    } else {
      cg_read_info = &full_read_info_;
    }

    ObStoreRowIterator *scanner = nullptr;
    ObMockDirectReadIterator sstable_iter;
    prepare_scan_param(*cg_read_info, trans_version_range, store_ctx, iter_param, context);
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param, context, range, scanner));
    ASSERT_NE(nullptr, scanner);
    ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, *cg_read_info));

    ObMockIterator res_iter;
    res_iter.reset();
    ASSERT_EQ(OB_SUCCESS, res_iter.from(result[i]));
    bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, false/*cmp multi version row flag*/);
    ASSERT_TRUE(is_equal);
    scanner->~ObStoreRowIterator();
  }
}

TEST_P(TestCOMerge, test_co_to_row_with_three_pure_co)
{
  ObCOMergeTestType type = GetParam();
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  const char *co_table_data1[1];
  co_table_data1[0]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "100          -8       0        12        EXIST   \n"
      "103          -8       0      NULL        EXIST  \n"
      "104          -8       0        3        EXIST   \n"
      "110          -8       0        2        EXIST   \n"
      "120          -8       0        3        EXIST   \n";

  const char *co_table_data2[1];
  co_table_data2[0]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "101          -9       0      NULL        EXIST   \n"
      "102          -9       0      NULL        EXIST  \n"
      "108          -9       0        4        EXIST   \n"
      "109          -9       0        6        EXIST   \n"
      "123          -9       0        8        EXIST   \n";

  const char *co_table_data3[1];
  co_table_data3[0]=
      "bigint     bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "96          -10       0      16        EXIST   \n"
      "99          -10       0      5        EXIST  \n"
      "106         -10       0      -12        EXIST   \n"
      "107         -10       0      23        EXIST   \n"
      "112         -10       0      NULL        EXIST   \n";

  const char *micro_data1[1];
  micro_data1[0] =
      "bigint     bigint   bigint   bigint    dml           flag    multi_version_row_flag\n"
      "94         -15       0        9        T_DML_INSERT  EXIST   CLF\n"
      "102        -14       0        NOP       T_DML_UPDATE  EXIST   CLF\n"
      "104        -15       0        NOP       T_DML_DELETE  DELETE   CLF\n"
      "105        -16       0        10       T_DML_INSERT  EXIST   CLF\n"
      "111        -20       0        NOP       T_DML_INSERT  EXIST   CLF\n"
      "120        -18       0        NOP       T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 1;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version);
  add_rowkey_and_each_column_group();
  ObArray<ObColDesc> col_ids;
  ASSERT_EQ(OB_SUCCESS, get_col_ids(table_schema_, col_ids));
  ASSERT_EQ(2, col_ids.count());

  // create co sstable 1
  ObMockIterator data_iter;
  data_iter.reset();
  OK(data_iter.from(co_table_data1[0]));
  ObTableHandleV2 co_table_handle1;
  const int64_t micro_row_count[3] = {2, 2, 1};
  const int64_t macro_row_count[3] = {2, 4, 3};
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle1, scn_range, false/*is_all_cg_base*/);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle1.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle1);

  // create co sstable 2
  data_iter.reset();
  OK(data_iter.from(co_table_data2[0]));
  ObTableHandleV2 co_table_handle2;
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle2, scn_range, false/*is_all_cg_base*/);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle2.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle2);

  // create co sstable 3
  data_iter.reset();
  OK(data_iter.from(co_table_data3[0]));
  ObTableHandleV2 co_table_handle3;
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle3, scn_range, false/*is_all_cg_base*/);
  ASSERT_EQ(3, static_cast<const ObCOSSTableV2 *>(co_table_handle3.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle3);

  ObTableHandleV2 handle1;
  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;

  //prepare merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
  merge_context.static_param_.co_static_param_.co_major_merge_type_ = ObCOMajorMergePolicy::BUILD_ROW_STORE_MERGE;
  merge_context.static_param_.merge_sstable_status_array_.at(0).co_major_sstable_status_ = PURE_COL;
  ASSERT_EQ(OB_SUCCESS, merge_context.static_param_.init_co_merge_flags());
  ASSERT_EQ(OB_SUCCESS, merge_context.prepare_mocked_row_store_cg_schema());
  ASSERT_EQ(OB_SUCCESS, merge_context.mock_row_store_table_read_info());
  merge_context.array_count_ = 1;
  merge_context.static_param_.is_full_merge_ = true;
  alloc_merge_infos(merge_context);
  OK(merge_context.prepare_index_builder(merge_context.base_rowkey_cg_idx_, merge_context.base_rowkey_cg_idx_ + 1));
  mix_type_test(type, merge_context, 0, merge_context.base_rowkey_cg_idx_, merge_context.base_rowkey_cg_idx_ + 1, true);

  // create all cg sstable
  OK(create_cg_sstables(merge_context, merge_context.base_rowkey_cg_idx_, merge_context.base_rowkey_cg_idx_ + 1));
  ASSERT_EQ(1, merge_context.merged_cg_tables_handle_.get_count());

  const char *result[1];
  result[0] =
      "bigint    bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "94          -15       0        9    EXIST   \n"
      "96          -10       0      16    EXIST   \n"
      "99          -10       0        5    EXIST   \n"
      "100          -8       0        12    EXIST   \n"
      "101          -9       0        NULL        EXIST   \n"
      "102          -14       0       NULL     EXIST   \n"
      "103          -8       0        NULL        EXIST   \n"
      "105          -16       0        10        EXIST   \n"
      "106          -10       0        -12        EXIST   \n"
      "107          -10       0        23        EXIST   \n"
      "108          -9       0        4        EXIST   \n"
      "109          -9       0        6        EXIST   \n"
      "110          -8       0        2        EXIST   \n"
      "111          -20       0        NULL        EXIST   \n"
      "112          -10       0        NULL        EXIST   \n"
      "120          -18       0        3        EXIST   \n"
      "123         -9       0        8        EXIST   \n";

  ObDatumRange range;
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;

  ObTableIterParam iter_param;
  ObTableAccessContext context;
  const ObITableReadInfo *cg_read_info = nullptr;
  ObStoreCtx store_ctx;

  ObSSTable *merged_sstable = static_cast<ObSSTable *>(merge_context.merged_cg_tables_handle_.get_table(0));
  ASSERT_NE(nullptr, merged_sstable);
  cg_read_info = &full_read_info_;

  ObStoreRowIterator *scanner = nullptr;
  ObMockDirectReadIterator sstable_iter;
  prepare_scan_param(*cg_read_info, trans_version_range, store_ctx, iter_param, context);
  ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param, context, range, scanner));
  ASSERT_NE(nullptr, scanner);

  ObMockIterator res_iter;
  res_iter.reset();

  ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, *cg_read_info));
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result[0]));
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, false/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
}

TEST_P(TestCOMerge, test_update_all_with_one_pure_co)
{
  ObCOMergeTestType type = GetParam();
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  const char *co_table_data[1];
  co_table_data[0]=
      "bigint     bigint   bigint   bigint  bigint    flag    multi_version_row_flag\n"
      "100          -8       0        12     5        EXIST   \n"
      "103          -8       0      NULL     6        EXIST   \n"
      "104          -8       0        3      7        EXIST   \n"
      "110          -8       0        2      8        EXIST   \n"
      "120          -8       0        3      9        EXIST   \n";

  const char *micro_data[1];
  micro_data[0] =
      "bigint     bigint   bigint   bigint  bigint      dml           flag   multi_version_row_flag\n"
      "100         -15       0        NOP    10      T_DML_UPDATE     EXIST   CLF\n"
      "103         -14       0        NOP    10      T_DML_UPDATE     EXIST   CLF\n"
      "104         -15       0        NOP    10      T_DML_UPDATE     EXIST   CLF\n"
      "110         -16       0        NOP    10      T_DML_UPDATE     EXIST   CLF\n"
      "120         -20       0        NOP    10      T_DML_UPDATE     EXIST   CLF\n";

  int schema_rowkey_cnt = 1;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  add_rowkey_and_each_column_group();
  ObArray<ObColDesc> col_ids;
  ASSERT_EQ(OB_SUCCESS, get_col_ids(table_schema_, col_ids));
  ASSERT_EQ(3, col_ids.count());

  // create co sstable
  ObMockIterator data_iter;
  data_iter.reset();
  OK(data_iter.from(co_table_data[0]));
  ObTableHandleV2 co_table_handle;
  const int64_t micro_row_count[3] = {2, 2, 1};
  const int64_t macro_row_count[3] = {2, 4, 3};
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle, scn_range, false/*is_all_cg_base*/);
  ASSERT_EQ(4, static_cast<const ObCOSSTableV2 *>(co_table_handle.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle);

  ObTableHandleV2 handle1;
  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;

  //prepare merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
  merge_context.array_count_ = 4;
  merge_context.static_param_.is_full_merge_ = true;
  alloc_merge_infos(merge_context);
  OK(merge_context.prepare_index_builder(0, 4));

  mix_type_test(type, merge_context, 0, 0, 4);

  // create all cg sstable
  OK(create_cg_sstables(merge_context, 0, 4));
  ASSERT_EQ(4, merge_context.merged_cg_tables_handle_.get_count());

  const char *result[4];
  result[0] =
      "bigint     bigint   bigint   bigint  bigint    flag    multi_version_row_flag\n"
      "100          -15       0        12     10      EXIST   \n"
      "103          -14       0      NULL     10      EXIST   \n"
      "104          -15       0        3      10      EXIST   \n"
      "110          -16       0        2      10      EXIST   \n"
      "120          -20       0        3      10      EXIST   \n";

  result[1] =
      "bigint    flag    multi_version_row_flag\n"
      "100         EXIST   \n"
      "103         EXIST   \n"
      "104         EXIST   \n"
      "110         EXIST   \n"
      "120         EXIST   \n";

  result[2] =
      "bigint    flag    multi_version_row_flag\n"
      "12        EXIST   \n"
      "NULL      EXIST   \n"
      "3         EXIST   \n"
      "2         EXIST   \n"
      "3         EXIST   \n";

  result[3] =
      "bigint    flag    multi_version_row_flag\n"
      "10        EXIST   \n"
      "10      EXIST   \n"
      "10         EXIST   \n"
      "10         EXIST   \n"
      "10         EXIST   \n";

  init_co_sstable(merge_context.merged_cg_tables_handle_, 4);
  ObArray<int32_t> out_col_project;
  for (int64_t i = 0; i < 4; i++) {
    ObDatumRange range;
    range.set_whole_range();
    trans_version_range.base_version_ = 1;
    trans_version_range.multi_version_start_ = 1;
    trans_version_range.snapshot_version_ = INT64_MAX;

    ObTableIterParam iter_param;
    ObTableAccessContext context;
    const ObITableReadInfo *cg_read_info = nullptr;
    ObStoreCtx store_ctx;

    ObSSTable *merged_sstable = static_cast<ObSSTable *>(merge_context.merged_cg_tables_handle_.get_table(i));
    ASSERT_NE(nullptr, merged_sstable);
    if (i > 0) {
      get_cg_read_info(col_ids.at(i - 1), cg_read_info);
    } else {
      ASSERT_EQ(OB_SUCCESS, merge_context.mock_row_store_table_read_info());
      cg_read_info = &merge_context.mocked_row_store_table_read_info_;
    }

    ObStoreRowIterator *scanner = nullptr;
    ObMockDirectReadIterator sstable_iter;
    prepare_scan_param(*cg_read_info, trans_version_range, store_ctx, iter_param, context);
    if (i == 0) {
      construct_out_cols(out_col_project, iter_param);
    }
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param, context, range, scanner));
    ASSERT_NE(nullptr, scanner);

    ObMockIterator res_iter;
    res_iter.reset();

    bool is_equal = true;
    if (i == 0) {
      ASSERT_EQ(OB_SUCCESS, res_iter.from_for_datum(result[i]));
      is_equal = compare_datum_row(&res_iter, scanner);
    } else {
      ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, *cg_read_info));
      ASSERT_EQ(OB_SUCCESS, res_iter.from(result[i]));
      is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, false/*cmp multi version row flag*/);
    }
    ASSERT_TRUE(is_equal);
    scanner->~ObStoreRowIterator();
  }
}

TEST_P(TestCOMerge, test_update_all_with_three_pure_co)
{
  ObCOMergeTestType type = GetParam();
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  const char *co_table_data1[1];
  co_table_data1[0]=
      "bigint     bigint   bigint   bigint  bigint   flag    multi_version_row_flag\n"
      "100          -8       0        12      5     EXIST   \n"
      "103          -8       0      NULL      5     EXIST  \n"
      "104          -8       0        3       5     EXIST   \n"
      "110          -8       0        2       5     EXIST   \n"
      "120          -8       0        3       5     EXIST   \n";

  const char *co_table_data2[1];
  co_table_data2[0]=
      "bigint     bigint   bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "101          -9       0      NULL      5       EXIST   \n"
      "102          -9       0      NULL      5       EXIST  \n"
      "108          -9       0        4       5       EXIST   \n"
      "109          -9       0        6       5       EXIST   \n"
      "123          -9       0        8       5       EXIST   \n";

  const char *co_table_data3[1];
  co_table_data3[0]=
      "bigint     bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "96          -10       0      16       5      EXIST   \n"
      "99          -10       0      5        5      EXIST  \n"
      "106         -10       0      -12      5      EXIST   \n"
      "107         -10       0      23       5      EXIST   \n"
      "112         -10       0      NULL     5      EXIST   \n";

  const char *micro_data1[1];
  micro_data1[0] =
      "bigint     bigint   bigint   bigint  bigint    dml           flag    multi_version_row_flag\n"
      "96         -15       0        NOP      10   T_DML_UPDATE     EXIST   CLF\n"
      "99         -15       0        NOP      10   T_DML_UPDATE     EXIST   CLF\n"
      "100        -15       0        NOP      10   T_DML_UPDATE     EXIST   CLF\n"
      "101        -15       0        NOP      10   T_DML_UPDATE     EXIST   CLF\n"
      "102        -15       0        NOP      10   T_DML_UPDATE     EXIST   CLF\n"
      "103        -15       0        NOP      10   T_DML_UPDATE     EXIST   CLF\n"
      "104        -15       0        NOP      10   T_DML_UPDATE     EXIST   CLF\n"
      "106        -15       0        NOP      10   T_DML_UPDATE     EXIST   CLF\n"
      "107        -15       0        NOP      10   T_DML_UPDATE     EXIST   CLF\n"
      "108        -15       0        NOP      10   T_DML_UPDATE     EXIST   CLF\n"
      "109        -15       0        NOP      10   T_DML_UPDATE     EXIST   CLF\n"
      "110        -15       0        NOP      10   T_DML_UPDATE     EXIST   CLF\n"
      "112        -15       0        NOP      10   T_DML_UPDATE     EXIST   CLF\n"
      "120        -15       0        NOP      10   T_DML_UPDATE     EXIST   CLF\n"
      "123        -15       0        NOP      10   T_DML_UPDATE     EXIST   CLF\n";

  int schema_rowkey_cnt = 1;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version);
  add_rowkey_and_each_column_group();
  ObArray<ObColDesc> col_ids;
  ASSERT_EQ(OB_SUCCESS, get_col_ids(table_schema_, col_ids));
  ASSERT_EQ(3, col_ids.count());

  // create co sstable 1
  ObMockIterator data_iter;
  data_iter.reset();
  OK(data_iter.from(co_table_data1[0]));
  ObTableHandleV2 co_table_handle1;
  const int64_t micro_row_count[3] = {2, 2, 1};
  const int64_t macro_row_count[3] = {2, 4, 3};
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle1, scn_range, false/*is_all_cg_base*/);
  ASSERT_EQ(4, static_cast<const ObCOSSTableV2 *>(co_table_handle1.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle1);

  // create co sstable 2
  data_iter.reset();
  OK(data_iter.from(co_table_data2[0]));
  ObTableHandleV2 co_table_handle2;
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle2, scn_range, false/*is_all_cg_base*/);
  ASSERT_EQ(4, static_cast<const ObCOSSTableV2 *>(co_table_handle2.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle2);

  // create co sstable 3
  data_iter.reset();
  OK(data_iter.from(co_table_data3[0]));
  ObTableHandleV2 co_table_handle3;
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle3, scn_range, false/*is_all_cg_base*/);
  ASSERT_EQ(4, static_cast<const ObCOSSTableV2 *>(co_table_handle3.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle3);

  ObTableHandleV2 handle1;
  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;

  //prepare merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
  merge_context.array_count_ = 4;
  merge_context.static_param_.is_full_merge_ = true;
  alloc_merge_infos(merge_context);
  OK(merge_context.prepare_index_builder(0, 4));

  mix_type_test(type, merge_context, 0, 0, 4);

  // create all cg sstable
  OK(create_cg_sstables(merge_context, 0, 4));
  ASSERT_EQ(4, merge_context.merged_cg_tables_handle_.get_count());

  const char *result[4];
  result[0] =
      "bigint    bigint   bigint   bigint  bigint flag    multi_version_row_flag\n"
      "96         -15       0        16     10   EXIST   \n"
      "99         -15       0        5      10   EXIST   \n"
      "100        -15       0        12     10   EXIST   \n"
      "101        -15       0        NULL   10   EXIST   \n"
      "102        -15       0        NULL   10   EXIST   \n"
      "103        -15       0        NULL   10   EXIST   \n"
      "104        -15       0        3      10   EXIST   \n"
      "106        -15       0        -12    10   EXIST   \n"
      "107        -15       0        23     10   EXIST   \n"
      "108        -15       0        4      10   EXIST   \n"
      "109        -15       0        6      10   EXIST   \n"
      "110        -15       0        2      10   EXIST   \n"
      "112        -15       0        NULL   10   EXIST   \n"
      "120        -15       0        3      10   EXIST   \n"
      "123        -15       0        8      10   EXIST   \n";

  result[1] =
      "bigint    flag    multi_version_row_flag\n"
      "96         EXIST   \n"
      "99         EXIST   \n"
      "100        EXIST   \n"
      "101         EXIST   \n"
      "102         EXIST   \n"
      "103         EXIST   \n"
      "104         EXIST   \n"
      "106         EXIST   \n"
      "107         EXIST   \n"
      "108         EXIST   \n"
      "109         EXIST   \n"
      "110         EXIST   \n"
      "112         EXIST   \n"
      "120         EXIST   \n"
      "123         EXIST   \n";

  result[2] =
      "bigint    flag    multi_version_row_flag\n"
      "16        EXIST   \n"
      "5         EXIST   \n"
      "12        EXIST   \n"
      "NULL      EXIST   \n"
      "NULL      EXIST   \n"
      "NULL      EXIST   \n"
      "3         EXIST   \n"
      "-12       EXIST   \n"
      "23        EXIST   \n"
      "4         EXIST   \n"
      "6         EXIST   \n"
      "2         EXIST   \n"
      "NULL      EXIST   \n"
      "3         EXIST   \n"
      "8         EXIST   \n";

  result[3] =
      "bigint    flag    multi_version_row_flag\n"
      "10        EXIST   \n"
      "10        EXIST   \n"
      "10        EXIST   \n"
      "10        EXIST   \n"
      "10        EXIST   \n"
      "10        EXIST   \n"
      "10        EXIST   \n"
      "10        EXIST   \n"
      "10        EXIST   \n"
      "10        EXIST   \n"
      "10        EXIST   \n"
      "10        EXIST   \n"
      "10        EXIST   \n"
      "10        EXIST   \n"
      "10        EXIST   \n";

  init_co_sstable(merge_context.merged_cg_tables_handle_, 4);
  ObArray<int32_t> out_col_project;
  for (int64_t i = 0; i < 4; i++) {
    ObDatumRange range;
    range.set_whole_range();
    trans_version_range.base_version_ = 1;
    trans_version_range.multi_version_start_ = 1;
    trans_version_range.snapshot_version_ = INT64_MAX;

    ObTableIterParam iter_param;
    ObTableAccessContext context;
    const ObITableReadInfo *cg_read_info = nullptr;
    ObStoreCtx store_ctx;

    ObSSTable *merged_sstable = static_cast<ObSSTable *>(merge_context.merged_cg_tables_handle_.get_table(i));
    ASSERT_NE(nullptr, merged_sstable);
    if (i > 0) {
      get_cg_read_info(col_ids.at(i - 1), cg_read_info);
    } else {
      ASSERT_EQ(OB_SUCCESS, merge_context.mock_row_store_table_read_info());
      cg_read_info = &merge_context.mocked_row_store_table_read_info_;
    }

    ObStoreRowIterator *scanner = nullptr;
    ObMockDirectReadIterator sstable_iter;
    prepare_scan_param(*cg_read_info, trans_version_range, store_ctx, iter_param, context);
    if (i == 0) {
      construct_out_cols(out_col_project, iter_param);
    }
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param, context, range, scanner));
    ASSERT_NE(nullptr, scanner);

    ObMockIterator res_iter;
    res_iter.reset();

    bool is_equal = true;
    if (i == 0) {
      ASSERT_EQ(OB_SUCCESS, res_iter.from_for_datum(result[i]));
      is_equal = compare_datum_row(&res_iter, scanner);
    } else {
      ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, *cg_read_info));
      ASSERT_EQ(OB_SUCCESS, res_iter.from(result[i]));
      is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, false/*cmp multi version row flag*/);
    }
    ASSERT_TRUE(is_equal);
    scanner->~ObStoreRowIterator();
  }
}

TEST_P(TestCOMerge, test_update_one_major)
{
  ObCOMergeTestType type = GetParam();
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  const char *co_table_data1[1];
  co_table_data1[0]=
      "bigint     bigint   bigint   bigint  bigint   flag    multi_version_row_flag\n"
      "1          -8       0        1       5     EXIST   \n"
      "2          -8       0        1       5     EXIST  \n"
      "3          -8       0        1       5     EXIST   \n"
      "4          -8       0        1       5     EXIST   \n"
      "5          -8       0        1       5     EXIST   \n"
      "6          -8       0        1       5     EXIST   \n"
      "7          -8       0        1       5     EXIST   \n"
      "8          -8       0        1       5     EXIST   \n"
      "9          -8       0        1       5     EXIST   \n"
      "10         -8       0        1       5     EXIST   \n";

  const char *co_table_data2[1];
  co_table_data2[0]=
      "bigint     bigint   bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "11          -9       0        2       5       EXIST   \n"
      "12          -9       0        2       5       EXIST  \n"
      "13          -9       0        2       5       EXIST   \n"
      "14          -9       0        2       5       EXIST   \n"
      "15          -9       0        2       5       EXIST   \n"
      "16          -9       0        2       5       EXIST   \n"
      "17          -9       0        2       5       EXIST   \n"
      "18          -9       0        2       5       EXIST   \n"
      "19          -9       0        2       5       EXIST   \n"
      "20          -9       0        2       5       EXIST   \n";

  const char *micro_data1[1];
  micro_data1[0] =
      "bigint     bigint   bigint   bigint  bigint    dml           flag    multi_version_row_flag\n"
      "1         -15       0        NOP      10   T_DML_UPDATE     EXIST   CLF\n"
      "2         -15       0        NOP      10   T_DML_UPDATE     EXIST   CLF\n"
      "3         -15       0        NOP      10   T_DML_UPDATE     EXIST   CLF\n"
      "4         -15       0        NOP      10   T_DML_UPDATE     EXIST   CLF\n"
      "5         -15       0        NOP      10   T_DML_UPDATE     EXIST   CLF\n"
      "6         -15       0        NOP      10   T_DML_UPDATE     EXIST   CLF\n"
      "7         -15       0        NOP      10   T_DML_UPDATE     EXIST   CLF\n"
      "8         -15       0        NOP      10   T_DML_UPDATE     EXIST   CLF\n"
      "9         -15       0        NOP      10   T_DML_UPDATE     EXIST   CLF\n"
      "10        -15       0        NOP      10   T_DML_UPDATE     EXIST   CLF\n";

  int schema_rowkey_cnt = 1;

  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version);
  add_rowkey_and_each_column_group();
  ObArray<ObColDesc> col_ids;
  ASSERT_EQ(OB_SUCCESS, get_col_ids(table_schema_, col_ids));
  ASSERT_EQ(3, col_ids.count());

  // create co sstable 1
  ObMockIterator data_iter;
  data_iter.reset();
  OK(data_iter.from(co_table_data1[0]));
  ObTableHandleV2 co_table_handle1;
  const int64_t micro_row_count[3] = {2, 2, 1};
  const int64_t macro_row_count[3] = {2, 4, 3};
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle1, scn_range, false/*is_all_cg_base*/);
  ASSERT_EQ(4, static_cast<const ObCOSSTableV2 *>(co_table_handle1.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle1);

  // create co sstable 2
  data_iter.reset();
  OK(data_iter.from(co_table_data2[0]));
  ObTableHandleV2 co_table_handle2;
  prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version, 0,
                      micro_row_count, macro_row_count, data_iter, co_table_handle2, scn_range, false/*is_all_cg_base*/);
  ASSERT_EQ(4, static_cast<const ObCOSSTableV2 *>(co_table_handle2.get_table())->cs_meta_.column_group_cnt_);
  merge_context.static_param_.tables_handle_.add_table(co_table_handle2);

  ObTableHandleV2 handle1;
  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;

  //prepare merge_ctx
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
  merge_context.array_count_ = 4;
  alloc_merge_infos(merge_context);
  OK(merge_context.prepare_index_builder(0, 4));

  mix_type_test(type, merge_context, 0, 0, 4);

  // create all cg sstable
  OK(create_cg_sstables(merge_context, 0, 4));
  ASSERT_EQ(4, merge_context.merged_cg_tables_handle_.get_count());

  const char *result[4];
  result[0] =
      "bigint    bigint   bigint   bigint   bigint  flag    multi_version_row_flag\n"
      "1         -15       0        1        10      EXIST   \n"
      "2         -15       0        1        10   EXIST   \n"
      "3         -15       0        1        10   EXIST   \n"
      "4         -15       0        1        10   EXIST   \n"
      "5         -15       0        1        10   EXIST   \n"
      "6         -15       0        1        10   EXIST   \n"
      "7         -15       0        1        10   EXIST   \n"
      "8         -15       0        1        10   EXIST   \n"
      "9         -15       0        1        10   EXIST   \n"
      "10        -15       0        1         10   EXIST   \n"
      "11        -9       0        2         5   EXIST   \n"
      "12        -9       0        2         5   EXIST   \n"
      "13        -9       0        2         5   EXIST   \n"
      "14        -9       0        2         5   EXIST   \n"
      "15        -9       0        2      5   EXIST   \n"
      "16        -9       0        2      5   EXIST   \n"
      "17        -9       0        2      5   EXIST   \n"
      "18        -9       0        2      5   EXIST   \n"
      "19        -9       0        2      5   EXIST   \n"
      "20        -9       0        2      5   EXIST   \n";

  result[1] =
      "bigint    flag    multi_version_row_flag\n"
      "1         EXIST   \n"
      "2         EXIST   \n"
      "3        EXIST   \n"
      "4         EXIST   \n"
      "5         EXIST   \n"
      "6         EXIST   \n"
      "7         EXIST   \n"
      "8         EXIST   \n"
      "9         EXIST   \n"
      "10         EXIST   \n"
      "11         EXIST   \n"
      "12         EXIST   \n"
      "13         EXIST   \n"
      "14         EXIST   \n"
      "15         EXIST   \n"
      "16         EXIST   \n"
      "17         EXIST   \n"
      "18         EXIST   \n"
      "19         EXIST   \n"
      "20         EXIST   \n";

  result[2] =
      "bigint    flag    multi_version_row_flag\n"
      "1        EXIST   \n"
      "1         EXIST   \n"
      "1        EXIST   \n"
      "1      EXIST   \n"
      "1      EXIST   \n"
      "1      EXIST   \n"
      "1         EXIST   \n"
      "1       EXIST   \n"
      "1        EXIST   \n"
      "1         EXIST   \n"
      "2         EXIST   \n"
      "2         EXIST   \n"
      "2      EXIST   \n"
      "2         EXIST   \n"
      "2         EXIST   \n"
      "2         EXIST   \n"
      "2         EXIST   \n"
      "2         EXIST   \n"
      "2         EXIST   \n"
      "2         EXIST   \n";

  result[3] =
      "bigint    flag    multi_version_row_flag\n"
      "10        EXIST   \n"
      "10        EXIST   \n"
      "10        EXIST   \n"
      "10        EXIST   \n"
      "10        EXIST   \n"
      "10        EXIST   \n"
      "10        EXIST   \n"
      "10        EXIST   \n"
      "10        EXIST   \n"
      "10        EXIST   \n"
      "5        EXIST   \n"
      "5        EXIST   \n"
      "5        EXIST   \n"
      "5        EXIST   \n"
      "5        EXIST   \n"
      "5        EXIST   \n"
      "5        EXIST   \n"
      "5        EXIST   \n"
      "5        EXIST   \n"
      "5        EXIST   \n";

  init_co_sstable(merge_context.merged_cg_tables_handle_, 4);
  ObArray<int32_t> out_col_project;
  for (int64_t i = 0; i < 4; i++) {
    ObDatumRange range;
    range.set_whole_range();
    trans_version_range.base_version_ = 1;
    trans_version_range.multi_version_start_ = 1;
    trans_version_range.snapshot_version_ = INT64_MAX;

    ObTableIterParam iter_param;
    ObTableAccessContext context;
    const ObITableReadInfo *cg_read_info = nullptr;
    ObStoreCtx store_ctx;

    ObSSTable *merged_sstable = static_cast<ObSSTable *>(merge_context.merged_cg_tables_handle_.get_table(i));
    ASSERT_NE(nullptr, merged_sstable);
    if (i > 0) {
      get_cg_read_info(col_ids.at(i - 1), cg_read_info);
    } else {
      ASSERT_EQ(OB_SUCCESS, merge_context.mock_row_store_table_read_info());
      cg_read_info = &merge_context.mocked_row_store_table_read_info_;
    }

    ObStoreRowIterator *scanner = nullptr;
    ObMockDirectReadIterator sstable_iter;
    prepare_scan_param(*cg_read_info, trans_version_range, store_ctx, iter_param, context);
    if (i == 0) {
      construct_out_cols(out_col_project, iter_param);
    }
    ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param, context, range, scanner));
    ASSERT_NE(nullptr, scanner);

    ObMockIterator res_iter;
    res_iter.reset();

    bool is_equal = true;
    if (i == 0) {
      ASSERT_EQ(OB_SUCCESS, res_iter.from_for_datum(result[i]));
      is_equal = compare_datum_row(&res_iter, scanner);
    } else {
      ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, *cg_read_info));
      ASSERT_EQ(OB_SUCCESS, res_iter.from(result[i]));
      is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, false/*cmp multi version row flag*/);
    }
    ASSERT_TRUE(is_equal);
    scanner->~ObStoreRowIterator();
  }
}

INSTANTIATE_TEST_CASE_P(
    COMergeSuite,
    TestCOMerge,
    ::testing::Values(
        ObCOMergeTestType::NORMAL,
        ObCOMergeTestType::USE_ROW_TMP_FILE,
        ObCOMergeTestType::USE_COLUMN_TMP_FILE,
        ObCOMergeTestType::NORMAL_WITH_BASE_REPLAY,
        ObCOMergeTestType::USE_ROW_TMP_FILE_WITH_BASE_REPLAY,
        ObCOMergeTestType::USE_COLUMN_TMP_FILE_WITH_BASE_REPLAY
    )
);

}
}


int main(int argc, char **argv)
{
  system("rm -rf test_co_merge.log*");
  OB_LOGGER.set_file_name("test_co_merge.log", true);
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
