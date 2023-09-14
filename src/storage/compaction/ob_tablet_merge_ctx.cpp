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

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "storage/blocksstable/ob_index_block_builder.h"
#include "storage/ob_storage_schema.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/compaction/ob_compaction_diagnose.h"
#include "storage/compaction/ob_sstable_merge_info_mgr.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/compaction/ob_medium_compaction_mgr.h"
#include "storage/compaction/ob_medium_compaction_func.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tablet/ob_tablet_medium_info_reader.h"
#include "storage/compaction/ob_medium_list_checker.h"

namespace oceanbase
{
using namespace share;
namespace compaction
{

/*
 *  ----------------------------------------------ObTabletMergeInfo--------------------------------------------------
 */

ObTabletMergeInfo::ObTabletMergeInfo()
  :  is_inited_(false),
     lock_(common::ObLatchIds::TABLET_MERGE_INFO_LOCK),
     block_ctxs_(),
     bloom_filter_block_ctx_(nullptr),
     bloomfilter_block_id_(),
     sstable_merge_info_(),
     allocator_("MergeContext", OB_MALLOC_MIDDLE_BLOCK_SIZE, MTL_ID()),
     index_builder_(nullptr)
{
}

ObTabletMergeInfo::~ObTabletMergeInfo()
{
  destroy();
}


void ObTabletMergeInfo::destroy()
{
  is_inited_ = false;

  for (int64_t i = 0; i < block_ctxs_.count(); ++i) {
    if (NULL != block_ctxs_.at(i)) {
      block_ctxs_.at(i)->~ObMacroBlocksWriteCtx();
    }
  }
  block_ctxs_.reset();

  bloomfilter_block_id_.reset();

  if (OB_NOT_NULL(index_builder_)) {
    index_builder_->~ObSSTableIndexBuilder();
    index_builder_ = NULL;
  }
  sstable_merge_info_.reset();
  allocator_.reset();
}

int ObTabletMergeInfo::init(const ObTabletMergeCtx &ctx, bool need_check)
{
  int ret = OB_SUCCESS;
  const int64_t concurrent_cnt = ctx.get_concurrent_cnt();
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_UNLIKELY(need_check && concurrent_cnt < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(concurrent_cnt));
  } else if (OB_FAIL(block_ctxs_.prepare_allocate(concurrent_cnt))) {
    LOG_WARN("failed to reserve block arrays", K(ret), K(concurrent_cnt));
  } else {
    for (int64_t i = 0; i < concurrent_cnt; ++i) {
      block_ctxs_[i] = NULL;
    }
    bloomfilter_block_id_.reset();
    build_sstable_merge_info(ctx);
    is_inited_ = true;
  }

  return ret;
}

int ObTabletMergeInfo::init(const ObTabletMergeCtx &ctx, const share::SCN &mds_table_flush_scn)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else {
    bloomfilter_block_id_.reset();
    build_sstable_merge_info(ctx);
    sstable_merge_info_.compaction_scn_ = mds_table_flush_scn.get_val_for_tx();
    sstable_merge_info_.merge_type_ = ObMergeType::MDS_TABLE_MERGE;
    is_inited_ = true;
  }

  return ret;
}

void ObTabletMergeInfo::build_sstable_merge_info(const ObTabletMergeCtx &ctx)
{
  sstable_merge_info_.tenant_id_ = MTL_ID();
  sstable_merge_info_.ls_id_ = ctx.param_.ls_id_;
  sstable_merge_info_.tablet_id_ = ctx.param_.tablet_id_;
  sstable_merge_info_.compaction_scn_ = ctx.get_compaction_scn();
  sstable_merge_info_.merge_start_time_ = ctx.start_time_;
  sstable_merge_info_.merge_type_ = ctx.is_tenant_major_merge_ ? ObMergeType::MAJOR_MERGE : ctx.param_.merge_type_;
  sstable_merge_info_.progressive_merge_round_ = ctx.progressive_merge_round_;
  sstable_merge_info_.progressive_merge_num_ = ctx.progressive_merge_num_;
  sstable_merge_info_.concurrent_cnt_ = ctx.get_concurrent_cnt();
  sstable_merge_info_.is_full_merge_ = ctx.is_full_merge_;
}

int ObTabletMergeInfo::add_macro_blocks(
    const int64_t idx,
    blocksstable::ObMacroBlocksWriteCtx *write_ctx,
    const ObSSTableMergeInfo &sstable_merge_info)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (idx < 0 || idx >= sstable_merge_info_.concurrent_cnt_ || OB_ISNULL(write_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid idx", K(ret), K(idx), "concurrent_cnt", sstable_merge_info_.concurrent_cnt_);
  } else if (NULL != block_ctxs_[idx]) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "block ctx is valid, fatal error", K(ret), K(idx));
  } else if (OB_FAIL(sstable_merge_info_.add(sstable_merge_info))) {
    LOG_WARN("failed to add sstable_merge_info", K(ret));
  } else if (OB_FAIL(new_block_write_ctx(block_ctxs_[idx]))) {
    LOG_WARN("failed to new block write ctx", K(ret));
  } else if (OB_FAIL(block_ctxs_[idx]->set(*write_ctx))) {
    LOG_WARN("failed to assign block arrays", K(ret), K(idx), K(write_ctx));
  }
  return ret;
}

int ObTabletMergeInfo::add_bloom_filter(blocksstable::ObMacroBlocksWriteCtx &bloom_filter_block_ctx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(1 != bloom_filter_block_ctx.get_macro_block_list().count()
              || !bloom_filter_block_ctx.get_macro_block_list().at(0).is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to add bloomfilter", K(bloom_filter_block_ctx), K(ret));
  } else if (OB_UNLIKELY(bloomfilter_block_id_.is_valid())) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "The bloom filter block id is inited, fatal error", K(ret));
  } else {
    bloomfilter_block_id_ = bloom_filter_block_ctx.get_macro_block_list().at(0);
  }

  return ret;
}

int ObTabletMergeInfo::prepare_index_builder(const ObDataStoreDesc &desc)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(!desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data store desc", K(ret), K(desc));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObSSTableIndexBuilder)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else if (OB_ISNULL(index_builder_ = new (buf) ObSSTableIndexBuilder())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to new ObSSTableIndexBuilder", K(ret));
  } else if (OB_FAIL(index_builder_->init(desc))) {
    LOG_WARN("failed to init index builder", K(ret), K(desc));
  }
  return ret;
}

int ObTabletMergeInfo::build_create_sstable_param(const ObTabletMergeCtx &ctx,
                                                  const ObSSTableMergeRes &res,
                                                  const MacroBlockId &bf_macro_id,
                                                  ObTabletCreateSSTableParam &param)
{
  int ret = OB_SUCCESS;
  ObArray<ObColDesc> columns;
  if (OB_UNLIKELY(!ctx.is_valid() || !res.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid merge ctx", K(ret), K(ctx), K(res));
  } else {
    ObITable::TableKey table_key;
    table_key.table_type_ = ctx.get_merged_table_type();
    table_key.tablet_id_ = ctx.param_.tablet_id_;
    if (is_major_merge_type(ctx.param_.merge_type_) || is_meta_major_merge(ctx.param_.merge_type_)) {
      table_key.version_range_.snapshot_version_ = ctx.sstable_version_range_.snapshot_version_;
    } else {
      table_key.scn_range_ = ctx.scn_range_;
    }
    param.table_key_ = table_key;
    param.sstable_logic_seq_ = ctx.sstable_logic_seq_;
    param.filled_tx_scn_ = ctx.merge_scn_;

    param.table_mode_ = ctx.get_schema()->get_table_mode_struct();
    param.index_type_ = ctx.get_schema()->get_index_type();
    param.rowkey_column_cnt_ = ctx.get_schema()->get_rowkey_column_num()
        + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    param.latest_row_store_type_ = ctx.get_schema()->get_row_store_type();
    if (is_minor_merge_type(ctx.param_.merge_type_)) {
      param.recycle_version_ = ctx.sstable_version_range_.base_version_;
    } else {
      param.recycle_version_ = 0;
    }
    param.schema_version_ = ctx.schema_ctx_.schema_version_;
    param.create_snapshot_version_ = ctx.create_snapshot_version_;
    param.progressive_merge_round_ = ctx.progressive_merge_round_;
    param.progressive_merge_step_ = std::min(
            ctx.progressive_merge_num_, ctx.progressive_merge_step_ + 1);

    ObSSTableMergeRes::fill_addr_and_data(res.root_desc_,
        param.root_block_addr_, param.root_block_data_);
    ObSSTableMergeRes::fill_addr_and_data(res.data_root_desc_,
        param.data_block_macro_meta_addr_, param.data_block_macro_meta_);
    param.is_meta_root_ = res.data_root_desc_.is_meta_root_;
    param.root_row_store_type_ = res.root_row_store_type_;
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
    if (0 == res.row_count_ && 0 == res.max_merged_trans_version_) {
      // empty mini table merged forcely
      param.max_merged_trans_version_ = ctx.sstable_version_range_.snapshot_version_;
    } else {
      param.max_merged_trans_version_ = res.max_merged_trans_version_;
    }
    param.contain_uncommitted_row_ = res.contain_uncommitted_row_;
    param.compressor_type_ = res.compressor_type_;
    param.encrypt_id_ = res.encrypt_id_;
    param.master_key_id_ = res.master_key_id_;
    param.nested_size_ = res.nested_size_;
    param.nested_offset_ = res.nested_offset_;
    param.data_block_ids_ = res.data_block_ids_;
    param.other_block_ids_ = res.other_block_ids_;
    param.ddl_scn_.set_min();
    MEMCPY(param.encrypt_key_, res.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
    if (is_major_merge_type(ctx.param_.merge_type_)) {
      if (OB_FAIL(param.column_checksums_.assign(res.data_column_checksums_))) {
        LOG_WARN("fail to fill column checksum", K(ret), K(res));
      }
    }

    if (OB_SUCC(ret) && ctx.param_.tablet_id_.is_ls_tx_data_tablet()) {
      ret = record_start_tx_scn_for_tx_data(ctx, param);
    }
  }
  return ret;
}

int ObTabletMergeInfo::record_start_tx_scn_for_tx_data(const ObTabletMergeCtx &ctx, ObTabletCreateSSTableParam &param)
{
  int ret = OB_SUCCESS;
  // set INT64_MAX for invalid check
  param.filled_tx_scn_.set_max();

  if (is_mini_merge(ctx.param_.merge_type_)) {
    // when this merge is MINI_MERGE, use the start_scn of the oldest tx data memtable as start_tx_scn
    ObTxDataMemtable *tx_data_memtable = nullptr;
    if (ctx.tables_handle_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tables handle is unexpected empty", KR(ret), K(ctx));
    } else if (OB_ISNULL(tx_data_memtable = (ObTxDataMemtable*)ctx.tables_handle_.get_table(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("table ptr is unexpected nullptr", KR(ret), K(ctx));
    } else {
      param.filled_tx_scn_ = tx_data_memtable->get_start_scn();
    }
  } else if (is_minor_merge(ctx.param_.merge_type_)) {
    // when this merge is MINOR_MERGE, use max_filtered_end_scn in filter if filtered some tx data
    ObTransStatusFilter *compaction_filter_ = (ObTransStatusFilter*)ctx.compaction_filter_;
    ObSSTableMetaHandle sstable_meta_hdl;
    ObSSTable *oldest_tx_data_sstable = static_cast<ObSSTable *>(ctx.tables_handle_.get_table(0));
    if (OB_ISNULL(oldest_tx_data_sstable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tx data sstable is unexpected nullptr", KR(ret));
    } else if (OB_FAIL(oldest_tx_data_sstable->get_meta(sstable_meta_hdl))) {
      LOG_WARN("fail to get sstable meta handle", K(ret));
    } else {
      param.filled_tx_scn_ = sstable_meta_hdl.get_sstable_meta().get_filled_tx_scn();

      if (OB_NOT_NULL(compaction_filter_)) {
        // if compaction_filter is valid, update filled_tx_log_ts if recycled some tx data
        SCN recycled_scn;
        if (compaction_filter_->get_max_filtered_end_scn() > SCN::min_scn()) {
          recycled_scn = compaction_filter_->get_max_filtered_end_scn();
        } else {
          recycled_scn = compaction_filter_->get_recycle_scn();
        }
        if (recycled_scn > param.filled_tx_scn_) {
          param.filled_tx_scn_ = recycled_scn;
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected merge type when merge tx data table", KR(ret), K(ctx));
  }

  return ret;
  }

int ObTabletMergeInfo::create_sstable(ObTabletMergeCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet merge info is not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid merge ctx", K(ret), K(ctx));
  } else {
    SMART_VARS_2((ObSSTableMergeRes, res), (ObTabletCreateSSTableParam, param)) {
      if (OB_ISNULL(index_builder_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null index builder", K(ret));
      } else if (OB_FAIL(index_builder_->close(res))) {
        LOG_WARN("fail to close index builder", K(ret));
      } else if (OB_FAIL(build_create_sstable_param(ctx, res, bloomfilter_block_id_, param))) {
        LOG_WARN("fail to build create sstable param", K(ret));
      } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(param, ctx.allocator_, ctx.merged_sstable_))) {
        LOG_WARN("fail to create sstable", K(ret), K(param));
      } else {
        ObSSTableMergeInfo &sstable_merge_info = ctx.merge_info_.get_sstable_merge_info();
        sstable_merge_info.compaction_scn_ = ctx.get_compaction_scn();
        (void)ctx.generate_participant_table_info(sstable_merge_info.participant_table_info_);
        (void)ctx.generate_macro_id_list(sstable_merge_info.macro_id_list_, sizeof(sstable_merge_info.macro_id_list_));

        FLOG_INFO("succeed to merge sstable", K(param),
                 "table_key", ctx.merged_sstable_.get_key(),
                 "sstable_merge_info", sstable_merge_info);
      }
    }
  }
  return ret;
}

int ObTabletMergeInfo::new_block_write_ctx(blocksstable::ObMacroBlocksWriteCtx *&ctx)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;

  if (OB_NOT_NULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ctx must be null", K(ret), K(ctx));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(blocksstable::ObMacroBlocksWriteCtx)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc ObMacroBlocksWriteCtx", K(ret));
  } else {
    ctx = new (buf) blocksstable::ObMacroBlocksWriteCtx();
  }
  return ret;
}

/*
 *  ----------------------------------------------ObCompactionTimeGuard--------------------------------------------------
 */
constexpr float ObCompactionTimeGuard::COMPACTION_SHOW_PERCENT_THRESHOLD;
const char *ObCompactionTimeGuard::ObTabletCompactionEventStr[] = {
    "WAIT_TO_SCHEDULE",
    "COMPACTION_POLICY",
    "GET_SCHEMA",
    "CALC_PROGRESSIVE_PARAM",
    "PRE_PROCESS_TX_TABLE",
    "GET_PARALLEL_RANGE",
    "EXECUTE",
    "CREATE_SSTABLE",
    "UPDATE_TABLET",
    "RELEASE_MEMTABLE",
    "SCHEDULE_OTHER_COMPACTION",
    "GET_TABLET",
    "DAG_FINISH"
};

const char *ObCompactionTimeGuard::get_comp_event_str(enum ObTabletCompactionEvent event)
{
  STATIC_ASSERT(static_cast<int64_t>(COMPACTION_EVENT_MAX) == ARRAYSIZEOF(ObTabletCompactionEventStr), "events str len is mismatch");
  const char *str = "";
  if (event >= COMPACTION_EVENT_MAX || event < DAG_WAIT_TO_SCHEDULE) {
    str = "invalid_type";
  } else {
    str = ObTabletCompactionEventStr[event];
  }
  return str;
}

ObCompactionTimeGuard::ObCompactionTimeGuard()
  : ObOccamTimeGuard(COMPACTION_WARN_THRESHOLD_RATIO, nullptr, nullptr, "[STORAGE] ")
{
}

ObCompactionTimeGuard::~ObCompactionTimeGuard()
{
}

int64_t ObCompactionTimeGuard::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int64_t total_cost = 0;
  if (idx_ > DAG_WAIT_TO_SCHEDULE && click_poinsts_[DAG_WAIT_TO_SCHEDULE] > COMPACTION_SHOW_TIME_THRESHOLD) {
    fmt_ts_to_meaningful_str(buf, buf_len, pos, "wait_schedule_time", click_poinsts_[DAG_WAIT_TO_SCHEDULE]);
  }
  for (int64_t idx = COMPACTION_POLICY; idx < idx_; ++idx) {
    total_cost += click_poinsts_[idx];
  }
  if (total_cost > COMPACTION_SHOW_TIME_THRESHOLD) {
    float ratio = 0;
    for (int64_t idx = COMPACTION_POLICY; idx < idx_; ++idx) {
      const uint64_t time_interval = click_poinsts_[idx];
      ratio = (float)(time_interval)/ total_cost;
      if (ratio >= COMPACTION_SHOW_PERCENT_THRESHOLD || time_interval >= COMPACTION_SHOW_TIME_THRESHOLD) {
        fmt_ts_to_meaningful_str(buf, buf_len, pos, get_comp_event_str((ObTabletCompactionEvent)line_array_[idx]), click_poinsts_[idx]);
        if (ratio > 0.01) {
          common::databuff_printf(buf, buf_len, pos, "(%.2f)",ratio);
        }
        common::databuff_printf(buf, buf_len, pos, "|");
      }
    }
  }
  fmt_ts_to_meaningful_str(buf, buf_len, pos, "total", total_cost);
  if (pos != 0 && pos < buf_len) {
    buf[pos - 1] = ';';
  }

  if (pos != 0 && pos < buf_len) {
    pos -= 1;
  }
  return pos;
}

void ObCompactionTimeGuard::add_time_guard(const ObCompactionTimeGuard &other)
{
  // last_click_ts_ is not useflu
  for (int i = 0; i < other.idx_; ++i) {
    if (line_array_[i] == other.line_array_[i]) {
      click_poinsts_[i] += other.click_poinsts_[i];
    } else {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "failed to add_time_guard", KPC(this), K(other));
      break;
    }
  }
}

ObCompactionTimeGuard & ObCompactionTimeGuard::operator=(const ObCompactionTimeGuard &other)
{
  last_click_ts_ = other.last_click_ts_;
  idx_ = other.idx_;
  for (int i = 0; i < other.idx_; ++i) {
    line_array_[i] = other.line_array_[i];
    click_poinsts_[i] = other.click_poinsts_[i];
  }
  return *this;
}
/*
 *  ----------------------------------------------ObTabletMergeCtx--------------------------------------------------
 */

ObSchemaMergeCtx::ObSchemaMergeCtx(ObArenaAllocator &allocator)
  : allocator_(allocator),
    schema_version_(0),
    storage_schema_(nullptr)
{
}

ObTabletMergeCtx::ObTabletMergeCtx(
    ObTabletMergeDagParam &param,
    common::ObArenaAllocator &allocator)
  : param_(param),
    allocator_(allocator),
    sstable_version_range_(),
    scn_range_(),
    merge_scn_(),
    create_snapshot_version_(0),
    tables_handle_(),
    merged_sstable_(),
    schema_ctx_(allocator),
    is_full_merge_(false),
    is_tenant_major_merge_(false),
    merge_level_(MICRO_BLOCK_MERGE_LEVEL),
    merge_info_(),
    parallel_merge_ctx_(),
    ls_handle_(),
    tablet_handle_(),
    sstable_logic_seq_(0),
    start_time_(0),
    progressive_merge_num_(0),
    progressive_merge_round_(0),
    progressive_merge_step_(0),
    schedule_major_(false),
    read_base_version_(0),
    merge_dag_(nullptr),
    merge_progress_(nullptr),
    compaction_filter_(nullptr),
    time_guard_(),
    rebuild_seq_(-1),
    data_version_(0),
    tnode_stat_(),
    need_parallel_minor_merge_(true)
{
  merge_scn_.set_max();
}

ObTabletMergeCtx::~ObTabletMergeCtx()
{
  destroy();
}

void ObTabletMergeCtx::destroy()
{
  if (OB_NOT_NULL(merge_progress_)) {
    merge_progress_->~ObPartitionMergeProgress();
    allocator_.free(merge_progress_);
    merge_progress_ = nullptr;
  }
  if (OB_NOT_NULL(compaction_filter_)) {
    compaction_filter_->~ObICompactionFilter();
    allocator_.free(compaction_filter_);
    compaction_filter_ = nullptr;
  }
  if (OB_NOT_NULL(schema_ctx_.storage_schema_)) {
    schema_ctx_.storage_schema_->~ObStorageSchema();
    schema_ctx_.storage_schema_ = nullptr;

    // TODO(@lixia.yq): ensure that the buffer corresponding to storage schema is always allocated by ObArenaAllocator
    // otherwise there will be memory leak here.
  }
  tables_handle_.reset();
  tablet_handle_.reset();
}

int ObTabletMergeCtx::init_merge_progress(bool is_major)
{
  int ret = OB_SUCCESS;
  void * buf = nullptr;
  if (is_major) {
    if (OB_ISNULL(buf = static_cast<int64_t*>(allocator_.alloc(sizeof(ObPartitionMajorMergeProgress))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc memory for merge progress", K(ret));
      } else {
        merge_progress_ = new(buf) ObPartitionMajorMergeProgress(allocator_);
      }
  } else if (OB_ISNULL(buf = static_cast<int64_t*>(allocator_.alloc(sizeof(ObPartitionMergeProgress))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc memory for merge progress", K(ret));
  } else {
    merge_progress_ = new(buf) ObPartitionMergeProgress(allocator_);
  }
  return ret;
}

bool ObTabletMergeCtx::is_valid() const
{
  return param_.is_valid()
         && !tables_handle_.empty()
         && create_snapshot_version_ >= 0
         && schema_ctx_.schema_version_ >= 0
         && NULL != schema_ctx_.storage_schema_
         && schema_ctx_.storage_schema_->is_valid()
         && sstable_logic_seq_ >= 0
         && progressive_merge_num_ >= 0
         && parallel_merge_ctx_.is_valid()
         && scn_range_.is_valid()
         && tablet_handle_.is_valid()
         && ls_handle_.is_valid();
}

bool ObTabletMergeCtx::need_rewrite_macro_block(const ObMacroBlockDesc &macro_desc) const
{
  bool res = false;
  if (macro_desc.is_valid_with_macro_meta()) {
    const int64_t block_merge_round = macro_desc.macro_meta_->val_.progressive_merge_round_;
    res = progressive_merge_num_ > 1
        && block_merge_round < progressive_merge_round_
        && progressive_merge_step_ < progressive_merge_num_;
  } else {
    res = false;
  }
  return res;
}

ObITable::TableType ObTabletMergeCtx::get_merged_table_type() const
{
  ObITable::TableType table_type = ObITable::MAX_TABLE_TYPE;

  if (is_major_merge_type(param_.merge_type_)) { // MAJOR_MERGE
    table_type = ObITable::TableType::MAJOR_SSTABLE;
  } else if (MINI_MERGE == param_.merge_type_) {
    table_type = ObITable::TableType::MINI_SSTABLE;
  } else if (META_MAJOR_MERGE == param_.merge_type_) {
    table_type = ObITable::TableType::META_MAJOR_SSTABLE;
  } else if (DDL_KV_MERGE == param_.merge_type_) {
    table_type = ObITable::TableType::DDL_DUMP_SSTABLE;
  } else { // MINOR_MERGE || HISTORY_MINOR_MERGE
    table_type = ObITable::TableType::MINOR_SSTABLE;
  }
  return table_type;
}

int ObTabletMergeCtx::init_parallel_merge()
{
  int ret = OB_SUCCESS;
  if (!parallel_merge_ctx_.is_valid() && OB_FAIL(parallel_merge_ctx_.init(*this))) {
    STORAGE_LOG(WARN, "Failed to init parallel merge context", K(ret));
  }
  return ret;
}

int ObTabletMergeCtx::get_merge_range(int64_t parallel_idx, ObDatumRange &merge_range)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!parallel_merge_ctx_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected invalid parallel merge ctx", K(ret), K_(parallel_merge_ctx));
  } else if (OB_FAIL(parallel_merge_ctx_.get_merge_range(parallel_idx, merge_range))) {
    STORAGE_LOG(WARN, "Failed to get merge range from parallel merge ctx", K(ret));
  }
  return ret;
}

int ObTabletMergeCtx::inner_init_for_medium()
{
  int ret = OB_SUCCESS;
  const ObMediumCompactionInfo *medium_info = nullptr;
  ObGetMergeTablesResult get_merge_table_result;
  bool is_schema_changed = false;
  if (OB_FAIL(get_merge_tables(get_merge_table_result))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to get merge tables", K(ret), KPC(this), K(get_merge_table_result));
    }
  } else if (FALSE_IT(time_guard_.click(ObCompactionTimeGuard::COMPACTION_POLICY))) {
  } else if (get_merge_table_result.handle_.get_count() > 1
      && !ObTenantTabletScheduler::check_tx_table_ready(
      *ls_handle_.get_ls(),
      get_merge_table_result.scn_range_.end_scn_)) {
    ret = OB_EAGAIN;
    LOG_INFO("tx table is not ready. waiting for max_decided_log_ts ...",
             KR(ret), "merge_scn", get_merge_table_result.scn_range_.end_scn_);
  } else if (OB_FAIL(init_get_medium_compaction_info(param_.merge_version_, get_merge_table_result, is_schema_changed))) {
    // have checked medium info inside
    LOG_WARN("failed to get medium compaction info", K(ret), KPC(this));
  } else if (OB_FAIL(get_basic_info_from_result(get_merge_table_result))) {
    LOG_WARN("failed to set basic info to ctx", K(ret), K(get_merge_table_result), KPC(this));
  } else if (OB_FAIL(cal_major_merge_param(get_merge_table_result, is_schema_changed))) {
    LOG_WARN("fail to cal major merge param", K(ret), KPC(this));
  }
  return ret;
}

int ObTabletMergeCtx::get_merge_tables(ObGetMergeTablesResult &get_merge_table_result)
{
  int ret = OB_SUCCESS;
  ObGetMergeTablesParam get_merge_table_param;
  get_merge_table_param.merge_type_ = param_.merge_type_;
  get_merge_table_param.merge_version_ = param_.merge_version_;
  if (OB_FAIL(ObPartitionMergePolicy::get_merge_tables[param_.merge_type_](
          get_merge_table_param,
          *ls_handle_.get_ls(),
          *tablet_handle_.get_obj(),
          get_merge_table_result))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to get merge tables", K(ret), KPC(this), K(get_merge_table_result));
    }
  }
  return ret;
}

int ObTabletMergeCtx::init_get_medium_compaction_info(
    const int64_t medium_snapshot,
    ObGetMergeTablesResult &get_merge_table_result,
    bool &is_schema_changed)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = tablet_handle_.get_obj();
  ObArenaAllocator temp_allocator("GetMediumInfo", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()); // for load medium info
  ObMediumCompactionInfo medium_info;
  ObMediumCompactionInfoKey medium_info_key(medium_snapshot);
  storage::ObTabletMediumInfoReader medium_info_reader(*tablet);
  if (OB_UNLIKELY(get_merge_table_result.handle_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result is empty", K(ret), KPC(this), K(get_merge_table_result));
  } else if (OB_FAIL(medium_info_reader.init(temp_allocator))) {
    LOG_WARN("failed to init medium info reader", K(ret), KPC(this));
  } else if (OB_FAIL(medium_info_reader.get_specified_medium_info(temp_allocator, medium_info_key, medium_info))) {
    LOG_WARN("failed to get specified scn info", K(ret), K(medium_snapshot));
  } else if (OB_UNLIKELY(!medium_info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("medium info is invalid", KR(ret), K(medium_info));
  } else if (medium_info.contain_parallel_range_
      && !parallel_merge_ctx_.is_valid()
      && OB_FAIL(parallel_merge_ctx_.init(medium_info))) { // may init twice after swap tablet
    LOG_WARN("failed to init parallel merge ctx", K(ret), K(medium_info));
  } else {
    void *buf = nullptr;
    if (OB_NOT_NULL(schema_ctx_.storage_schema_)) {
      // do nothing
    } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObStorageSchema)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc storage schema", K(ret));
    } else {
      ObStorageSchema *storage_schema = nullptr;
      storage_schema = new(buf) ObStorageSchema();
      if (OB_FAIL(storage_schema->init(allocator_, medium_info.storage_schema_))) {
        LOG_WARN("failed to init storage schema from current medium info", K(ret), K(medium_info));
      } else {
        schema_ctx_.storage_schema_ = storage_schema;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(tablet_handle_.get_obj()->get_multi_version_start() > medium_snapshot)) {
      ret = OB_SNAPSHOT_DISCARDED;
      LOG_ERROR("multi version data is discarded, should not compaction now", K(ret), K(param_), K(medium_snapshot));
    }
  }
  if (FAILEDx(ObMediumListChecker::check_next_schedule_medium(&medium_info,
      get_merge_table_result.handle_.get_table(0)->get_snapshot_version(),
      true/*force_check*/))) {
    LOG_WARN("failed to check medium info and last major sstable", KR(ret), K(medium_info), K(get_merge_table_result));
  } else {
    schema_ctx_.schema_version_ = medium_info.storage_schema_.schema_version_;
    data_version_ = medium_info.data_version_;
    is_tenant_major_merge_ = medium_info.is_major_compaction();
  }
  return ret;
}

int ObTabletMergeCtx::inner_init_for_mini(bool &skip_rest_operation)
{
  int ret = OB_SUCCESS;
  skip_rest_operation = false;
  int64_t multi_version_start = 0;
  int64_t min_reserved_snapshot = 0;
  ObGetMergeTablesParam get_merge_table_param;
  ObGetMergeTablesResult get_merge_table_result;
  get_merge_table_param.merge_type_ = param_.merge_type_;
  get_merge_table_param.merge_version_ = param_.merge_version_;
  ObTablet *tablet = tablet_handle_.get_obj();

  if (OB_UNLIKELY(!is_mini_merge(param_.merge_type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid merge type", K(ret), K_(param));
  } else if (OB_FAIL(ObPartitionMergePolicy::get_merge_tables[param_.merge_type_](
          get_merge_table_param,
          *ls_handle_.get_ls(),
          *tablet,
          get_merge_table_result))) {
    // TODO(@DanLin) optimize this interface
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to get merge tables", K(ret), KPC(this), K(get_merge_table_result));
    }
  } else if (FALSE_IT(time_guard_.click(ObCompactionTimeGuard::COMPACTION_POLICY))) {
  } else if (get_merge_table_result.update_tablet_directly_) {
    skip_rest_operation = true;
    if (OB_FAIL(update_tablet_or_release_memtable(get_merge_table_result))) {
      LOG_WARN("failed to update tablet directly", K(ret), K(get_merge_table_result));
    }
  } else if (!ObTenantTabletScheduler::check_tx_table_ready(
      *ls_handle_.get_ls(),
      get_merge_table_result.scn_range_.end_scn_)) {
    ret = OB_EAGAIN;
    LOG_INFO("tx table is not ready. waiting for max_decided_log_ts ...",
        KR(ret), "merge_scn", get_merge_table_result.scn_range_.end_scn_);
  } else if (OB_FAIL(get_schema_and_gene_from_result(get_merge_table_result))) {
    LOG_WARN("Fail to get storage schema", K(ret), K(get_merge_table_result), KPC(this));
  }
  return ret;
}

int ObTabletMergeCtx::get_schema_and_gene_from_result(const ObGetMergeTablesResult &get_merge_table_result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_storage_schema_to_merge(get_merge_table_result.handle_))) {
    LOG_WARN("failed to get storage schema to merge", K(ret), KPC(this));
  } else if (OB_FAIL(get_basic_info_from_result(get_merge_table_result))) {
    LOG_WARN("failed to set basic info to ctx", K(ret), K(get_merge_table_result), KPC(this));
  } else if (OB_FAIL(cal_minor_merge_param())) {
    LOG_WARN("failed to cal minor merge param", K(ret), KPC(this));
  } else if (FALSE_IT(merge_scn_ = scn_range_.end_scn_)) {
  } else if (!is_minor_merge_type(get_merge_table_result.suggest_merge_type_)) {
  } else if (OB_UNLIKELY(scn_range_.is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Unexcepted empty scn range in minor merge", K(ret), K(scn_range_));
  }
  return ret;
}

int ObTabletMergeCtx::update_tablet_or_release_memtable(const ObGetMergeTablesResult &get_merge_table_result)
{
  int ret = OB_SUCCESS;
  ObTablet *old_tablet = tablet_handle_.get_obj();

  if (OB_UNLIKELY(!is_mini_merge(param_.merge_type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can only update tablet in mini merge", K(ret), KPC(this));
  } else if (OB_UNLIKELY(get_merge_table_result.scn_range_.end_scn_ > old_tablet->get_tablet_meta().clog_checkpoint_scn_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can't have larger end_log_ts", K(ret), K(get_merge_table_result), K(old_tablet->get_tablet_meta()));
  } else if (OB_FAIL(get_storage_schema_to_merge(get_merge_table_result.handle_))) {
    LOG_WARN("failed to get storage schema", K(ret), K(get_merge_table_result));
  } else if (OB_ISNULL(schema_ctx_.storage_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage schema is unexpected null", K(ret), KPC(this));
  } else if (OB_FAIL(update_tablet_directly(get_merge_table_result))) {
    LOG_WARN("failed to update tablet directly", K(ret), K(get_merge_table_result));
  }
  return ret;
}

int ObTabletMergeCtx::update_tablet_directly(const ObGetMergeTablesResult &get_merge_table_result)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  scn_range_ = get_merge_table_result.scn_range_;

  ObUpdateTableStoreParam param(
      nullptr/*sstable*/,
      get_merge_table_result.version_range_.snapshot_version_,
      get_merge_table_result.version_range_.multi_version_start_,
      schema_ctx_.storage_schema_,
      rebuild_seq_,
      true/*need_check_transfer_seq*/,
      tablet_handle_.get_obj()->get_tablet_meta().transfer_info_.transfer_seq_,
      is_major_merge_type(param_.merge_type_),
      SCN::min_scn()/*clog_checkpoint_scn*/);
  ObTabletHandle new_tablet_handle;
  if (OB_FAIL(ls_handle_.get_ls()->update_tablet_table_store(
      param_.tablet_id_, param, new_tablet_handle))) {
    LOG_WARN("failed to update tablet table store", K(ret), K(param));
  } else if (OB_TMP_FAIL(new_tablet_handle.get_obj()->release_memtables(new_tablet_handle.get_obj()->get_tablet_meta().clog_checkpoint_scn_))) {
    LOG_WARN("failed to release memtable", K(tmp_ret), K(param));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(schema_ctx_.storage_schema_)) {
  } else if (OB_FAIL(merge_info_.init(*this, false/*need_check*/))) {
    LOG_WARN("failed to inie merge info", K(ret));
  } else if (OB_FAIL(tables_handle_.assign(get_merge_table_result.handle_))) { // assgin for generate_participant_table_info
    LOG_WARN("failed to assgin table handle", K(ret));
  } else {
    merge_info_.get_sstable_merge_info().merge_finish_time_ = common::ObTimeUtility::fast_current_time();
    (void)generate_participant_table_info(merge_info_.get_sstable_merge_info().participant_table_info_);
    (void)merge_dag_->get_ctx()->collect_running_info();

    if (OB_TMP_FAIL(ObMediumCompactionScheduleFunc::schedule_tablet_medium_merge(
        *ls_handle_.get_ls(), *new_tablet_handle.get_obj()))) {
      if (OB_SIZE_OVERFLOW != tmp_ret && OB_EAGAIN != tmp_ret) {
        LOG_WARN("failed to schedule tablet adaptive merge", K(tmp_ret),
            "ls_id", param_.ls_id_, "tablet_id", param_.tablet_id_);
      }
    }
  }
  return ret;
}

int ObTabletMergeCtx::get_basic_info_from_result(
   const ObGetMergeTablesResult &get_merge_table_result)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(rebuild_seq_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebuild seq do not set, get tables failed", K(ret), K(rebuild_seq_));
  } else if (OB_FAIL(tables_handle_.assign(get_merge_table_result.handle_))) {
    LOG_WARN("failed to add tables", K(ret));
  } else if (OB_UNLIKELY(tables_handle_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected empty table handle", K(ret), K(tables_handle_));
  } else {
    sstable_version_range_ = get_merge_table_result.version_range_;
    scn_range_ = get_merge_table_result.scn_range_;
    if (param_.merge_type_ != get_merge_table_result.suggest_merge_type_) {
      FLOG_INFO("change merge type", "param", param_,
        "suggest_merge_type", get_merge_table_result.suggest_merge_type_);
      param_.merge_type_ = get_merge_table_result.suggest_merge_type_;
    }
    if (is_major_merge_type(param_.merge_type_)) {
      param_.report_ = GCTX.ob_service_;
    }
    const ObITable *table = nullptr;

    if (is_major_merge_type(param_.merge_type_) || is_mini_merge(param_.merge_type_)) {
      sstable_logic_seq_ = 0;
    } else if (OB_ISNULL(table = tables_handle_.get_table(tables_handle_.get_count() - 1)) || !table->is_sstable()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected table type", K(ret), KPC(table), K(tables_handle_));
    } else {
      const ObSSTable *sstable = static_cast<const ObSSTable *>(table);
      ObSSTableMetaHandle meta_handle;
      if (OB_FAIL(sstable->get_meta(meta_handle))) {
        LOG_WARN("get meta handle fail", K(ret), KPC(sstable));
      } else {
        sstable_logic_seq_ = MIN(ObMacroDataSeq::MAX_SSTABLE_SEQ, meta_handle.get_sstable_meta().get_sstable_seq()+ 1);
      }
    }

    create_snapshot_version_ = get_merge_table_result.create_snapshot_version_;
    schedule_major_ = get_merge_table_result.schedule_major_;
  }
  return ret;
}

int ObTabletMergeCtx::cal_minor_merge_param()
{
  int ret = OB_SUCCESS;
  //some input param check
  if (OB_UNLIKELY(tables_handle_.empty() || NULL == tables_handle_.get_table(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tables handle is invalid", K(ret), K(tables_handle_));
  } else {
    progressive_merge_num_ = 0;
    //determine whether to use increment/full merge
    is_full_merge_ = false;
    merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
    read_base_version_ = 0;
  }
  return ret;
}

int ObTabletMergeCtx::cal_major_merge_param(
  const ObGetMergeTablesResult &get_merge_table_result,
  const bool is_schema_changed)
{
  int ret = OB_SUCCESS;
  ObSSTable *base_table = nullptr;
  int64_t full_stored_col_cnt = 0;
  ObSSTableMetaHandle sstable_meta_hdl;

  read_base_version_ = get_merge_table_result.read_base_version_;
  param_.merge_version_ = get_merge_table_result.merge_version_;

  if (tables_handle_.empty()
      || NULL == (base_table = static_cast<ObSSTable*>(tables_handle_.get_table(0)))
      || (!base_table->is_major_sstable() && !base_table->is_meta_major_sstable())) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("base table must be major or meta major", K(ret), K(tables_handle_));
  } else if (OB_FAIL(base_table->get_meta(sstable_meta_hdl))) {
    LOG_WARN("fail to get sstable meta", K(ret), KPC(base_table));
  } else if (OB_FAIL(get_schema()->get_stored_column_count_in_sstable(full_stored_col_cnt))) {
    LOG_WARN("failed to get stored column count in sstable", K(ret), KPC(get_schema()));
  } else if (OB_UNLIKELY(sstable_meta_hdl.get_sstable_meta().get_column_count() > full_stored_col_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("stored col cnt in curr schema is less than old major sstable", K(ret),
        "col_cnt_in_sstable", sstable_meta_hdl.get_sstable_meta().get_column_count(),
        "col_cnt_in_schema", full_stored_col_cnt,
        K(sstable_meta_hdl), KPC(this));
  } else {
    if (1 == get_schema()->get_progressive_merge_num()) {
      is_full_merge_ = true;
    } else {
      is_full_merge_ = false;
    }
    const ObSSTableBasicMeta &base_meta = sstable_meta_hdl.get_sstable_meta().get_basic_meta();

    const int64_t meta_progressive_merge_round = base_meta.progressive_merge_round_;
    const int64_t schema_progressive_merge_round = get_schema()->get_progressive_merge_round();
    if (0 == get_schema()->get_progressive_merge_num()) {
      progressive_merge_num_ = (1 == schema_progressive_merge_round) ? 0 : OB_AUTO_PROGRESSIVE_MERGE_NUM;
    } else {
      progressive_merge_num_ = get_schema()->get_progressive_merge_num();
    }

    if (is_full_merge_) {
      progressive_merge_round_ = schema_progressive_merge_round;
      progressive_merge_step_ = progressive_merge_num_;
    } else if (meta_progressive_merge_round < schema_progressive_merge_round) { // new progressive merge
      progressive_merge_round_ = schema_progressive_merge_round;
      progressive_merge_step_ = 0;
    } else if (meta_progressive_merge_round == schema_progressive_merge_round) {
      progressive_merge_round_ = meta_progressive_merge_round;
      progressive_merge_step_ = base_meta.progressive_merge_step_;
    }
    FLOG_INFO("Calc progressive param", K(is_schema_changed), K(progressive_merge_num_),
        K(progressive_merge_round_), K(meta_progressive_merge_round), K(progressive_merge_step_),
        K(is_full_merge_), K(full_stored_col_cnt), K(sstable_meta_hdl.get_sstable_meta().get_column_count()));
  }

  if (OB_SUCC(ret)) {
    if (is_full_merge_ || (merge_level_ != MACRO_BLOCK_MERGE_LEVEL && is_schema_changed)) {
      merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
    }
  }
  return ret;
}

int ObTabletMergeCtx::init_merge_info()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_parallel_merge())) {
    LOG_WARN("failed to init parallel merge in sstable merge ctx", K(ret));
  } else if (OB_FAIL(merge_info_.init(*this))) {
    LOG_WARN("failed to init merge context", K(ret));
  } else {
    if (OB_NOT_NULL(compaction_filter_) && compaction_filter_->is_full_merge_) {
      is_full_merge_ = true;
    }
    time_guard_.click(ObCompactionTimeGuard::GET_PARALLEL_RANGE);
  }
  return ret;
}

int ObTabletMergeCtx::get_storage_schema_to_merge(const ObTablesHandleArray &merge_tables_handle)
{
  int ret = OB_SUCCESS;
  const ObMergeType &merge_type = param_.merge_type_;
  const ObStorageSchema *schema_on_tablet = nullptr;
  int64_t max_column_cnt_in_memtable = 0;
  int64_t max_column_cnt_on_recorder = 0;
  int64_t max_schema_version_in_memtable = 0;
  int64_t column_cnt_in_schema = 0;
  bool use_schema_on_tablet = true; // for minor & tx_mini, use storage schema on tablet

  if (OB_FAIL(tablet_handle_.get_obj()->load_storage_schema(allocator_, schema_on_tablet))) {
    LOG_WARN("failed to load storage schema", K(ret), K_(tablet_handle));
  } else if (0 == merge_tables_handle.get_count()) {
    // do nothing, only need to release memtable
  } else if (is_mini_merge(merge_type) && !param_.tablet_id_.is_ls_inner_tablet()) {
    if (OB_FAIL(schema_on_tablet->get_store_column_count(column_cnt_in_schema, true/*full_col*/))) {
      LOG_WARN("failed to get store column count", K(ret), K(column_cnt_in_schema));
    }
    ObITable *table = nullptr;
    memtable::ObMemtable *memtable = nullptr;
    for (int i = merge_tables_handle.get_count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      if (OB_ISNULL(table = merge_tables_handle.get_table(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table in tables_handle is invalid", K(ret), KPC(table));
      } else if (OB_ISNULL(memtable = dynamic_cast<memtable::ObMemtable *>(table))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table pointer does not point to a ObMemtable object", KPC(table));
      } else if (OB_FAIL(memtable->get_schema_info(column_cnt_in_schema,
          max_schema_version_in_memtable, max_column_cnt_in_memtable))) {
        LOG_WARN("failed to get schema info from memtable", KR(ret), KPC(memtable));
      }
    } // end of for

    if (FAILEDx(tablet_handle_.get_obj()->get_max_column_cnt_on_schema_recorder(max_column_cnt_on_recorder))) {
      LOG_WARN("failed to get max column cnt on schema recorder", KR(ret));
    } else if (max_column_cnt_in_memtable <= column_cnt_in_schema
            && max_column_cnt_on_recorder <= column_cnt_in_schema
            && max_schema_version_in_memtable <= schema_on_tablet->get_schema_version()) {
      // do nothing
    } else {
      // need alloc new storage schema & set column cnt
      void *buf = nullptr;
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObStorageSchema)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc storage schema", K(ret));
      } else {
        ObStorageSchema *storage_schema = new(buf) ObStorageSchema();
        if (OB_FAIL(storage_schema->init(allocator_, *schema_on_tablet, true/*column_info_simplified*/))) {
          LOG_WARN("failed to init storage schema", K(ret), K(schema_on_tablet));
          allocator_.free(storage_schema);
          storage_schema = nullptr;
        } else {
          // only update column cnt by memtable, use schema version on tablet_schema
          storage_schema->update_column_cnt(MAX(max_column_cnt_on_recorder, max_column_cnt_in_memtable));
          storage_schema->schema_version_ = MAX(max_schema_version_in_memtable, schema_on_tablet->get_schema_version());
          schema_ctx_.storage_schema_ = storage_schema;
          use_schema_on_tablet = false;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (use_schema_on_tablet) {
      schema_ctx_.storage_schema_ = schema_on_tablet;
    }
    schema_ctx_.schema_version_ = schema_ctx_.storage_schema_->get_schema_version();
    FLOG_INFO("get storage schema to merge", K_(param), K_(schema_ctx), K(use_schema_on_tablet),
      K(max_column_cnt_in_memtable), K(max_schema_version_in_memtable), K(max_column_cnt_on_recorder));
    if (!use_schema_on_tablet) {
      // destroy loaded schema memory after print log
      ObTablet::free_storage_schema(allocator_, schema_on_tablet);
    }
  }
  return ret;
}

int ObTabletMergeCtx::prepare_index_tree()
{
  int ret = OB_SUCCESS;
  ObDataStoreDesc desc;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid merge ctx", K(ret), KPC(this));
  } else if (OB_FAIL(desc.init_as_index(*get_schema(),
                               param_.ls_id_,
                               param_.tablet_id_,
                               param_.merge_type_,
                               sstable_version_range_.snapshot_version_,
                               data_version_))) {
    LOG_WARN("failed to init index store desc", K(ret), KPC(this));
  } else if (OB_FAIL(merge_info_.prepare_index_builder(desc))) {
    LOG_WARN("failed to prepare index builder", K(ret), K(desc));
  }
  return ret;
}

int ObTabletMergeCtx::prepare_merge_progress()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(merge_progress_)) {
    if (OB_FAIL(merge_progress_->init(this))) {
      merge_progress_->reset();
      LOG_WARN("failed to init merge progress", K(ret));
    } else {
      LOG_INFO("succeed to init merge progress", K(ret), KPC(merge_progress_));
    }
  }
  return ret;
}

bool ObTabletMergeCtx::need_swap_tablet(const ObTablet &tablet,
                                             const int64_t row_count,
                                             const int64_t macro_count)
{
  return tablet.get_memtable_mgr()->has_memtable()
    && (row_count >= LARGE_VOLUME_DATA_ROW_COUNT_THREASHOLD
      || macro_count >= LARGE_VOLUME_DATA_MACRO_COUNT_THREASHOLD);
}

int ObTabletMergeCtx::try_swap_tablet_handle()
{
  int ret = OB_SUCCESS;
  // check need swap tablet when compaction
  if (OB_UNLIKELY(is_mini_merge(param_.merge_type_))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("mini merge not support swap tablet", K(ret), K_(param));
  } else {
    ObTablesHandleArray &tables_handle = tables_handle_;
    int64_t row_count = 0;
    int64_t macro_count = 0;
    const ObSSTable *sstable = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle_.get_count(); ++i) {
      sstable = static_cast<const ObSSTable*>(tables_handle_.get_table(i));
      ObSSTableMetaHandle meta_handle;
      if (OB_FAIL(sstable->get_meta(meta_handle))) {
        LOG_WARN("get sstable meta handle fail", K(ret));
      } else {
        row_count += meta_handle.get_sstable_meta().get_row_count();
        macro_count += meta_handle.get_sstable_meta().get_data_macro_block_count();
      }
    } // end of for
    if (need_swap_tablet(*tablet_handle_.get_obj(), row_count, macro_count)) {
      tables_handle_.reset(); // clear tables array
      const ObTabletMapKey key(param_.ls_id_, param_.tablet_id_);
      if (OB_FAIL(MTL(ObTenantMetaMemMgr*)->get_tablet_with_allocator(
        WashTabletPriority::WTP_LOW, key, allocator_, tablet_handle_, true/*force_alloc_new*/))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_TABLET_NOT_EXIST;
        } else {
          LOG_WARN("failed to get alloc tablet handle", K(ret), K(key));
        }
      } else if (OB_FAIL(inner_init_for_medium())) {
        LOG_WARN("failed to init for medium", K(ret), K(param_));
      } else if (OB_FAIL(tablet_handle_.get_obj()->clear_memtables_on_table_store())) {
        LOG_WARN("failed to clear memtables on table_store", K(ret), K(param_));
      } else {
        LOG_INFO("success to swap tablet handle", K(ret), K(tablet_handle_), K(tables_handle_));
      }
    }
  }
  return ret;
}

int ObTabletMergeCtx::generate_participant_table_info(PartTableInfo &info) const
{
  int ret = OB_SUCCESS;
  info.is_major_merge_ = is_major_merge_type(param_.merge_type_);
  if (info.is_major_merge_) {
    info.table_cnt_ = static_cast<int32_t>(tables_handle_.get_count());
    info.snapshot_version_ = tables_handle_.get_table(0)->get_snapshot_version();
    if (tables_handle_.get_count() > 1) {
      info.start_scn_ = tables_handle_.get_table(1)->get_start_scn().get_val_for_tx();
      info.end_scn_ = tables_handle_.get_table(tables_handle_.get_count() - 1)->get_end_scn().get_val_for_tx();
    }
  } else {
    if (tables_handle_.get_count() > 0) {
      info.table_cnt_ = static_cast<int32_t>(tables_handle_.get_count());
      info.start_scn_ = tables_handle_.get_table(0)->get_start_scn().get_val_for_tx();
      info.end_scn_ = tables_handle_.get_table(tables_handle_.get_count() - 1)->get_end_scn().get_val_for_tx();
    }
  }
  return ret;
}

int ObTabletMergeCtx::generate_macro_id_list(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObMacroIdIterator iter;
  const ObSSTable *new_table = nullptr;
  ObSSTableMetaHandle sst_meta_hdl;
  if (OB_FAIL(merged_sstable_.get_meta(sst_meta_hdl))) {
    LOG_WARN("failed to get sstable meta", K(ret));
  } else if (OB_FAIL(sst_meta_hdl.get_sstable_meta().get_macro_info().get_data_block_iter(iter))) {
    LOG_WARN("fail to get data block iterator", K(ret), K(merged_sstable_));
  } else {
    MEMSET(buf, '\0', buf_len);
    int pret = 0;
    const int64_t macro_count = sst_meta_hdl.get_sstable_meta().get_data_macro_block_count();
    int64_t remain_len = buf_len;
    if (macro_count < 40) {
      MacroBlockId macro_id;
      for (int64_t i = 0; OB_SUCC(ret) && OB_SUCC(iter.get_next_macro_id(macro_id)); ++i) {
        if (0 == i) {
          pret = snprintf(buf + strlen(buf), remain_len, "%ld", macro_id.second_id());
        } else {
          pret = snprintf(buf + strlen(buf), remain_len, ",%ld", macro_id.second_id());
        }
        if (pret < 0 || pret > remain_len) {
          ret = OB_BUF_NOT_ENOUGH;
        } else {
          remain_len -= pret;
        }
      } // end of for
    }
  }
  return ret;
}

void ObTabletMergeCtx::collect_running_info()
{
  int tmp_ret = OB_SUCCESS;
  ObDagWarningInfo warning_info;
  ObSSTableMergeInfo &sstable_merge_info = merge_info_.get_sstable_merge_info();
  sstable_merge_info.dag_id_ = merge_dag_->get_dag_id();

  ADD_COMPACTION_INFO_PARAM(sstable_merge_info.comment_, sizeof(sstable_merge_info.comment_),
      "time_guard", time_guard_);

  const int64_t dag_key = merge_dag_->hash();
  // calc flush macro speed
  uint64_t exe_ts = time_guard_.get_specified_cost_time(ObCompactionTimeGuard::EXECUTE);
  if (exe_ts > 0 && sstable_merge_info.new_flush_occupy_size_ > 0) {
    sstable_merge_info.new_flush_data_rate_ = (int)(((float)sstable_merge_info.new_flush_occupy_size_/ 1024) / ((float)exe_ts / 1_s));
  }

  ObInfoParamBuffer info_buffer;
  if (OB_SUCCESS == MTL(ObDagWarningHistoryManager *)->get_with_param(dag_key, &warning_info, info_buffer)) {
    sstable_merge_info.dag_ret_ = warning_info.dag_ret_;
    sstable_merge_info.task_id_ = warning_info.task_id_;
    sstable_merge_info.retry_cnt_ = warning_info.retry_cnt_;
    warning_info.info_param_ = nullptr;
  }

  ObScheduleSuspectInfo ret_info;
  int64_t suspect_info_hash = ObScheduleSuspectInfo::gen_hash(MTL_ID(), dag_key);
  info_buffer.reuse();
  if (OB_SUCCESS == MTL(compaction::ObScheduleSuspectInfoMgr *)->get_with_param(suspect_info_hash, &ret_info, info_buffer)) {
    sstable_merge_info.add_time_ = ret_info.add_time_;
    sstable_merge_info.info_param_ = ret_info.info_param_;
    if (OB_TMP_FAIL(MTL(compaction::ObScheduleSuspectInfoMgr *)->delete_info(suspect_info_hash))) {
      LOG_WARN_RET(tmp_ret, "failed to delete old suspect info ", K(tmp_ret), K(sstable_merge_info));
    }
  }

  if (OB_TMP_FAIL(MTL(storage::ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(sstable_merge_info))) {
    LOG_WARN_RET(tmp_ret, "failed to add sstable merge info ", K(tmp_ret), K(sstable_merge_info));
  }
  sstable_merge_info.info_param_ = nullptr;
}


} // namespace compaction
} // namespace oceanbase
