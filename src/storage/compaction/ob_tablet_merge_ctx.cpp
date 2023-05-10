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
#include "src/storage/meta_mem/ob_tenant_meta_mem_mgr.h"

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
     allocator_("MergeContext", OB_MALLOC_MIDDLE_BLOCK_SIZE),
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
    sstable_merge_info_.tenant_id_ = MTL_ID();
    sstable_merge_info_.ls_id_ = ctx.param_.ls_id_;
    sstable_merge_info_.tablet_id_ = ctx.param_.tablet_id_;
    sstable_merge_info_.compaction_scn_ = ctx.get_compaction_scn();
    sstable_merge_info_.merge_start_time_ = ObTimeUtility::fast_current_time();
    sstable_merge_info_.merge_type_ = ctx.is_tenant_major_merge_ ? MAJOR_MERGE : ctx.param_.merge_type_;
    sstable_merge_info_.progressive_merge_round_ = ctx.progressive_merge_round_;
    sstable_merge_info_.progressive_merge_num_ = ctx.progressive_merge_num_;
    sstable_merge_info_.concurrent_cnt_ = ctx.get_concurrent_cnt();
    sstable_merge_info_.is_full_merge_ = ctx.is_full_merge_;
    is_inited_ = true;
  }

  return ret;
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
    param.root_row_store_type_ = res.root_desc_.row_type_;
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
      if (FAILEDx(res.fill_column_checksum(ctx.get_schema(), param.column_checksums_))) {
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
    ObSSTable *oldest_tx_data_sstable = static_cast<ObSSTable *>(ctx.tables_handle_.get_table(0));
    if (OB_ISNULL(oldest_tx_data_sstable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tx data sstable is unexpected nullptr", KR(ret));
    } else {
      param.filled_tx_scn_ = oldest_tx_data_sstable->get_filled_tx_scn();

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
    int64_t column_count;
    if (OB_FAIL(ctx.get_schema()->get_store_column_count(
                column_count, is_multi_version_merge(ctx.param_.merge_type_)))) {
      LOG_WARN("fail to get store column count", K(ret), K(ctx));
    } else {
      SMART_VARS_2((ObSSTableMergeRes, res), (ObTabletCreateSSTableParam, param)) {
        column_count += ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
        if (OB_ISNULL(index_builder_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null index builder", K(ret));
        } else if (OB_FAIL(index_builder_->close(column_count, res))) {
          LOG_WARN("fail to close index builder", K(ret), K(column_count));
        } else if (OB_FAIL(build_create_sstable_param(ctx, res, bloomfilter_block_id_, param))) {
          LOG_WARN("fail to build create sstable param", K(ret));
        } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(param, ctx.merged_table_handle_))) {
          LOG_WARN("fail to create sstable", K(ret), K(param));
        } else {

          ObSSTableMergeInfo &sstable_merge_info = ctx.merge_info_.get_sstable_merge_info();
          sstable_merge_info.compaction_scn_ = ctx.get_compaction_scn();
          (void)ctx.generate_participant_table_info(sstable_merge_info.participant_table_str_, sizeof(sstable_merge_info.participant_table_str_));
          (void)ctx.generate_macro_id_list(sstable_merge_info.macro_id_list_, sizeof(sstable_merge_info.macro_id_list_));

          FLOG_INFO("succeed to merge sstable", K(param),
                  "table_key", ctx.merged_table_handle_.get_table()->get_key(),
                   "sstable_merge_info", sstable_merge_info);
        }
      }
    }
  }
  return ret;
}


int ObTabletMergeInfo::get_data_macro_block_count(int64_t &macro_block_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletMergeInfo has not been inited", K(ret));
  } else {
    macro_block_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < block_ctxs_.count(); i++) {
      if (OB_NOT_NULL(block_ctxs_.at(i))) {
        macro_block_count += block_ctxs_.at(i)->macro_block_list_.count();
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
      const uint32_t time_interval = click_poinsts_[idx];
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

ObSchemaMergeCtx::ObSchemaMergeCtx(ObIAllocator &allocator)
  : allocator_(allocator),
    base_schema_version_(0),
    schema_version_(0),
    allocated_storage_schema_(false),
    storage_schema_(nullptr)
{
}

ObTabletMergeCtx::ObTabletMergeCtx(
    ObTabletMergeDagParam &param,
    common::ObIAllocator &allocator)
  : param_(param),
    allocator_(allocator),
    sstable_version_range_(),
    scn_range_(),
    merge_scn_(),
    create_snapshot_version_(0),
    tables_handle_(),
    merged_table_handle_(),
    schema_ctx_(allocator),
    is_full_merge_(false),
    is_tenant_major_merge_(false),
    merge_level_(MICRO_BLOCK_MERGE_LEVEL),
    merge_info_(),
    parallel_merge_ctx_(),
    ls_handle_(),
    tablet_handle_(),
    sstable_logic_seq_(0),
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
    merge_list_(),
    tnode_stat_()
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
         && schema_ctx_.base_schema_version_ >= 0
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
  ObGetMergeTablesParam get_merge_table_param;
  ObGetMergeTablesResult get_merge_table_result;
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
  } else if (FALSE_IT(time_guard_.click(ObCompactionTimeGuard::COMPACTION_POLICY))) {
  } else if (get_merge_table_result.handle_.get_count() > 1
      && !ObTenantTabletScheduler::check_tx_table_ready(
      *ls_handle_.get_ls(),
      get_merge_table_result.scn_range_.end_scn_)) {
    ret = OB_EAGAIN;
    LOG_INFO("tx table is not ready. waiting for max_decided_log_ts ...",
             KR(ret), "merge_scn", get_merge_table_result.scn_range_.end_scn_);
  } else if (OB_FAIL(init_get_medium_compaction_info(param_.merge_version_, get_merge_table_result))) { // have checked medium info inside
    LOG_WARN("failed to get medium compaction info", K(ret), KPC(this));
  } else if (OB_FAIL(get_basic_info_from_result(get_merge_table_result))) {
    LOG_WARN("failed to set basic info to ctx", K(ret), K(get_merge_table_result), KPC(this));
  } else if (OB_FAIL(cal_major_merge_param(get_merge_table_result))) {
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
    ObGetMergeTablesResult &get_merge_table_result)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = tablet_handle_.get_obj();
  const ObMediumCompactionInfoList &medium_list = tablet->get_medium_compaction_info_list();
  const bool medium_in_storage = medium_snapshot <= tablet->get_tablet_meta().max_serialized_medium_scn_;
  ObMediumCompactionInfo medium_info;
  const ObMediumCompactionInfo *medium_info_ptr = &medium_info;
  if (OB_UNLIKELY(!medium_list.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("medium compaction mgr is invalid", K(ret), KPC(this), K(medium_list));
  } else if (medium_in_storage && OB_FAIL(medium_list.get_specified_scn_info(medium_snapshot, medium_info_ptr))) {
    LOG_WARN("failed to get medium info from mgr", K(ret), K(medium_snapshot), K(medium_list));
  } else if (!medium_in_storage && OB_FAIL(get_specified_medium_compaction_info_from_memtable(allocator_, medium_snapshot, medium_info))) {
    LOG_WARN("failed to get medium info from memtable", K(ret), K(medium_snapshot));
  } else if (OB_UNLIKELY(nullptr == medium_info_ptr || !medium_info_ptr->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("medium compaction info is invalid", K(ret), KPC(this), K(medium_list), KPC(medium_info_ptr));
  } else if (medium_info_ptr->contain_parallel_range_
      && !parallel_merge_ctx_.is_valid()
      && OB_FAIL(parallel_merge_ctx_.init(*medium_info_ptr))) { // may init twice after swap tablet
    LOG_WARN("failed to init parallel merge ctx", K(ret), KPC(medium_info_ptr));
  } else {
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObStorageSchema)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc storage schema", K(ret));
    } else {
      ObStorageSchema *storage_schema = nullptr;
      storage_schema = new(buf) ObStorageSchema();
      schema_ctx_.storage_schema_ = storage_schema;
      schema_ctx_.allocated_storage_schema_ = true;
      if (OB_FAIL(storage_schema->init(allocator_, medium_info_ptr->storage_schema_))) {
        LOG_WARN("failed to init storage schema from current medium info", K(ret), KPC(medium_info_ptr));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (tablet_handle_.get_obj()->get_multi_version_start() > medium_snapshot) {
      ret = OB_SNAPSHOT_DISCARDED;
      LOG_ERROR("multi version data is discarded, should not compaction now", K(ret), K(param_), K(medium_snapshot));
    }
  }
  if (OB_SUCC(ret)) {
    get_merge_table_result.schema_version_ = medium_info_ptr->storage_schema_.schema_version_;
    data_version_ = medium_info_ptr->data_version_;
    is_tenant_major_merge_ = medium_info_ptr->is_major_compaction();
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
    } else { // OB_NO_NEED_MERGE
      int tmp_ret = OB_SUCCESS;
      // then release memtable
      if (OB_TMP_FAIL(tablet->release_memtables(tablet->get_tablet_meta().clog_checkpoint_scn_))) {
        LOG_WARN("failed to release memtable", K(tmp_ret), K(tablet->get_tablet_meta().clog_checkpoint_scn_));
      }
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
  if (OB_FAIL(get_storage_schema_to_merge(get_merge_table_result.handle_, true/*get_schema_on_memtable*/))) {
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
  // check whether snapshot is updated or have storage_schema
  bool update_table_store_flag = false;
  if (OB_UNLIKELY(!is_mini_merge(param_.merge_type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can only update tablet in mini merge", K(ret), KPC(this));
  } else if (OB_FAIL(get_storage_schema_to_merge(get_merge_table_result.handle_, true/*get_schema_on_memtable*/))) {
    LOG_WARN("failed to get storage schema", K(ret), K(get_merge_table_result));
  } else if (OB_ISNULL(schema_ctx_.storage_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage schema is unexpected null", K(ret), KPC(this));
  } else if (schema_ctx_.storage_schema_->get_schema_version() > old_tablet->get_storage_schema().get_schema_version()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can't have larger storage schema", K(ret), K(schema_ctx_.storage_schema_), K(old_tablet->get_storage_schema()));
  } else if (OB_UNLIKELY(get_merge_table_result.scn_range_.end_scn_ > old_tablet->get_tablet_meta().clog_checkpoint_scn_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can't have larger end_log_ts", K(ret), K(get_merge_table_result), K(old_tablet->get_tablet_meta()));
  } else if (get_merge_table_result.version_range_.snapshot_version_ > old_tablet->get_snapshot_version()) {
    // need write slog to update snapshot_version on tablet_meta
    update_table_store_flag = true;
  }

  const SCN &release_memtable_scn = old_tablet->get_clog_checkpoint_scn();
  if (OB_FAIL(ret)) {
  } else if (update_table_store_flag && OB_FAIL(update_tablet_directly(get_merge_table_result))) {
    LOG_WARN("failed to update tablet directly", K(ret), K(get_merge_table_result), K(update_table_store_flag));
  } else if (OB_FAIL(old_tablet->release_memtables(release_memtable_scn))) {
    LOG_WARN("failed to release memtable", K(ret), K(release_memtable_scn));
  } else {
    LOG_INFO("success to release memtable", K(ret), K_(param), K(release_memtable_scn));
  }
  return ret;
}

int ObTabletMergeCtx::update_tablet_directly(const ObGetMergeTablesResult &get_merge_table_result)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  scn_range_ = get_merge_table_result.scn_range_;

  ObTableHandleV2 empty_table_handle;
  ObUpdateTableStoreParam param(
      empty_table_handle,
      get_merge_table_result.version_range_.snapshot_version_,
      get_merge_table_result.version_range_.multi_version_start_,
      schema_ctx_.storage_schema_,
      rebuild_seq_,
      is_major_merge_type(param_.merge_type_),
      SCN::min_scn()/*clog_checkpoint_scn*/);
  ObTabletHandle new_tablet_handle;
  if (OB_FAIL(ls_handle_.get_ls()->update_tablet_table_store(
      param_.tablet_id_, param, new_tablet_handle))) {
    LOG_WARN("failed to update tablet table store", K(ret), K(param));
  } else if (OB_FAIL(merge_info_.init(*this, false/*need_check*/))) {
    LOG_WARN("failed to inie merge info", K(ret));
  } else if (OB_FAIL(tables_handle_.assign(get_merge_table_result.handle_))) { // assgin for generate_participant_table_info
    LOG_WARN("failed to assgin table handle", K(ret));
  } else {
    merge_info_.get_sstable_merge_info().merge_finish_time_ = common::ObTimeUtility::fast_current_time();
    (void)generate_participant_table_info(merge_info_.get_sstable_merge_info().participant_table_str_,
        sizeof(merge_info_.get_sstable_merge_info().participant_table_str_));
    (void)merge_dag_->get_ctx().collect_running_info();

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
      sstable_logic_seq_ = MIN(ObMacroDataSeq::MAX_SSTABLE_SEQ, static_cast<const ObSSTable *>(table)->get_sstable_seq() + 1);
    }

    schema_ctx_.base_schema_version_ = get_merge_table_result.base_schema_version_;
    schema_ctx_.schema_version_ = get_merge_table_result.schema_version_;
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

int ObTabletMergeCtx::cal_major_merge_param(const ObGetMergeTablesResult &get_merge_table_result)
{
  int ret = OB_SUCCESS;
  ObSSTable *base_table = nullptr;
  bool is_schema_changed = false;

  read_base_version_ = get_merge_table_result.read_base_version_;
  param_.merge_version_ = get_merge_table_result.merge_version_;

  if (tables_handle_.empty()
      || NULL == (base_table = static_cast<ObSSTable*>(tables_handle_.get_table(0)))
      || (!base_table->is_major_sstable() && !base_table->is_meta_major_sstable())) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("base table must be major or meta major", K(ret), K(tables_handle_));
  } else {
    if (1 == get_schema()->get_progressive_merge_num()) {
      is_full_merge_ = true;
    } else {
      is_full_merge_ = false;
    }
    const ObSSTableBasicMeta &base_meta = base_table->get_meta().get_basic_meta();
    if (base_table->get_meta().get_column_count() != get_schema()->get_column_count()
        || base_meta.compressor_type_ != get_schema()->get_compressor_type()
        || (ObRowStoreType::DUMMY_ROW_STORE != base_meta.latest_row_store_type_
          && base_meta.latest_row_store_type_ != get_schema()->row_store_type_)) {
      is_schema_changed = true;
    }

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
        K(is_full_merge_));
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

int ObTabletMergeCtx::get_medium_compaction_info_to_store()
{
  int ret = OB_SUCCESS;
  if (is_mini_merge(param_.merge_type_)) {
    ObSEArray<ObITable*, MAX_MEMSTORE_CNT> memtables;
    if (OB_FAIL(tables_handle_.get_tables(memtables))) {
      LOG_WARN("failed to get tables", K(ret), K(memtables));
    } else if (OB_FAIL(ObMediumCompactionScheduleFunc::get_medium_info_list_from_memtable(
      allocator_, memtables, merge_list_))) {
      LOG_WARN("failed to get medium info list from memtable", K(ret));
    } else if (merge_list_.size() > 0) {
      LOG_INFO("success get medium info list", "ls_id", param_.ls_id_,
          "tablet_id", param_.tablet_id_, K(merge_list_));
    }
  }
  return ret;
}

int ObTabletMergeCtx::get_specified_medium_compaction_info_from_memtable(
  ObIAllocator &allocator,
  const int64_t medium_snapshot,
  ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  ObSEArray<ObITable*, MAX_MEMSTORE_CNT> memtables;
  ObMediumCompactionInfoList tmp_medium_list;
  if (OB_FAIL(tablet_handle_.get_obj()->get_memtables(memtables, true/*need_active*/))) {
    LOG_WARN("failed to get memtables", K(ret), K(param_));
  } else if (memtables.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("no memtable", K(ret), K(memtables), K(param_));
  } else if (OB_FAIL(ObMediumCompactionScheduleFunc::get_medium_info_list_from_memtable(
      tmp_allocator, memtables, tmp_medium_list))) {
    LOG_WARN("failed to get medium info list from memtable", K(ret));
  } else if (tmp_medium_list.is_empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("have memtables, but medium list is empty", K(ret), K(memtables), K(tmp_medium_list));
  } else {
    const ObMediumCompactionInfo *info_in_list = nullptr;
    DLIST_FOREACH_X(info, tmp_medium_list.get_list(), OB_SUCC(ret)) {
      if (OB_UNLIKELY(memtable::MultiSourceDataUnitType::MEDIUM_COMPACTION_INFO != info->type())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("return info is invalid", K(ret), KPC(info));
      } else if (FALSE_IT(info_in_list = static_cast<const ObMediumCompactionInfo *>(info))) {
      } else if (medium_snapshot < info_in_list->medium_snapshot_) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("not found specified medium info in medium list", K(ret), K(param_), K(memtables), K(tmp_medium_list));
      } else if (medium_snapshot == info_in_list->medium_snapshot_) {
        if (OB_FAIL(medium_info.init(allocator, *info_in_list))) {
          LOG_WARN("failed to init medium info", K(ret));
        }
        break;
      }
    }
    if (OB_SUCC(ret) && medium_info.is_valid()) {
      LOG_INFO("success get medium info", "ls_id", param_.ls_id_,
          "tablet_id", param_.tablet_id_, K(medium_info));
    }
  }
  return ret;
}

int ObTabletMergeCtx::get_storage_schema_to_merge(
    const ObTablesHandleArray &merge_tables_handle,
    const bool get_schema_on_memtable)
{
  int ret = OB_SUCCESS;
  const ObMergeType &merge_type = param_.merge_type_;
  ObStorageSchema *storage_schema = nullptr;

  bool get_storage_schema_flag = true;
  if (is_mini_merge(merge_type) && get_schema_on_memtable) {
    void *buf = nullptr;
    ObSEArray<ObITable*, MAX_MEMSTORE_CNT> memtables;

    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObStorageSchema)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc storage schema", K(ret));
    } else {
      storage_schema = new(buf) ObStorageSchema();
    }

    if (FAILEDx(merge_tables_handle.get_tables(memtables))) {
      LOG_WARN("failed to get tables", K(ret), K(memtables));
    } else if (OB_FAIL(ObMediumCompactionScheduleFunc::get_latest_storage_schema_from_memtable(
        allocator_, memtables, *storage_schema))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get storage schema on memtable", K(ret), K_(param));
      }
    } else {
      get_storage_schema_flag = false;
    }

    // free alloced storage schema
    if ((OB_FAIL(ret) || get_storage_schema_flag) && nullptr != storage_schema) {
      storage_schema->~ObStorageSchema();
      allocator_.free(storage_schema);
      storage_schema = nullptr;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (get_storage_schema_flag) {
    schema_ctx_.storage_schema_ = &tablet_handle_.get_obj()->get_storage_schema();
  } else {
    OB_ASSERT(nullptr != storage_schema);
    schema_ctx_.storage_schema_ = storage_schema;
    schema_ctx_.allocated_storage_schema_ = true;
  }

  if (OB_SUCC(ret)) {
    OB_ASSERT(nullptr != schema_ctx_.storage_schema_);
    schema_ctx_.schema_version_ = schema_ctx_.storage_schema_->get_schema_version();
    FLOG_INFO("get storage schema to merge", "ls_id", param_.ls_id_,
        "tablet_id", param_.tablet_id_, K_(schema_ctx), K(get_storage_schema_flag),
        K(get_schema_on_memtable));
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
  } else if (OB_FAIL(desc.init(*get_schema(),
                               param_.ls_id_,
                               param_.tablet_id_,
                               param_.merge_type_,
                               sstable_version_range_.snapshot_version_,
                               data_version_))) {
    LOG_WARN("failed to init index store desc", K(ret), KPC(this));
  } else {
    // TODO(zhuixin.gsy) modify index_desc.init to avoid reset col_desc_array_
    desc.row_column_count_ = desc.rowkey_column_count_ + 1;
    desc.col_desc_array_.reset();
    desc.need_prebuild_bloomfilter_ = false;
    if (OB_FAIL(desc.col_desc_array_.init(desc.row_column_count_))) {
      LOG_WARN("failed to reserve column desc array", K(ret));
    } else if (OB_FAIL(get_schema()->get_rowkey_column_ids(desc.col_desc_array_))) {
      LOG_WARN("failed to get rowkey column ids", K(ret));
    } else if (OB_FAIL(ObMultiVersionRowkeyHelpper::add_extra_rowkey_cols(desc.col_desc_array_))) {
      LOG_WARN("failed to get extra rowkey column ids", K(ret));
    } else {
      ObObjMeta meta;
      meta.set_varchar();
      meta.set_collation_type(CS_TYPE_BINARY);
      share::schema::ObColDesc col;
      col.col_id_ = static_cast<uint64_t>(desc.row_column_count_ + OB_APP_MIN_COLUMN_ID);
      col.col_type_ = meta;
      col.col_order_ = DESC;

      if (OB_FAIL(desc.col_desc_array_.push_back(col))) {
        LOG_WARN("failed to push back last col for index", K(ret), K(col));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(merge_info_.prepare_index_builder(desc))) {
      LOG_WARN("failed to prepare index builder", K(ret), K(desc));
    }
  }
  return ret;
}

int ObTabletMergeCtx::prepare_merge_progress()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(merge_progress_)) {
    const ObTableReadInfo &read_info = tablet_handle_.get_obj()->get_full_read_info();
    if (OB_FAIL(merge_progress_->init(this, read_info))) {
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
    const ObSSTable *table = nullptr;
    for (int64_t i = 0; i < tables_handle_.get_count(); ++i) {
      table = static_cast<const ObSSTable*>(tables_handle_.get_table(i));
      row_count += table->get_meta().get_row_count();
      macro_count += table->get_meta().get_basic_meta().get_data_macro_block_count();
    }
    if (need_swap_tablet(*tablet_handle_.get_obj(), row_count, macro_count)) {
      tables_handle_.reset(); // clear tables array
      const ObTabletMapKey key(param_.ls_id_, param_.tablet_id_);
      if (OB_FAIL(MTL(ObTenantMetaMemMgr*)->get_tablet_with_allocator(
        WashTabletPriority::WTP_LOW, key, allocator_, tablet_handle_, true/*force_alloc_new*/))) {
        LOG_WARN("failed to get alloc tablet handle", K(ret), K(key));
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

int ObTabletMergeCtx::generate_participant_table_info(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (is_major_merge_type(param_.merge_type_)) {
    ADD_COMPACTION_INFO_PARAM(buf, buf_len,
        "table_cnt", tables_handle_.get_count(),
        "[MAJOR]scn", tables_handle_.get_table(0)->get_snapshot_version());
    if (tables_handle_.get_count() > 1) {
      ADD_COMPACTION_INFO_PARAM(buf, buf_len,
          "[MINI]start_scn", tables_handle_.get_table(1)->get_start_scn().get_val_for_tx(),
          "end_scn", tables_handle_.get_table(tables_handle_.get_count() - 1)->get_end_scn().get_val_for_tx());
    }
  } else {
    if (tables_handle_.get_count() > 0) {
      ADD_COMPACTION_INFO_PARAM(buf, buf_len,
          "table_cnt", tables_handle_.get_count(),
          "start_scn", tables_handle_.get_table(0)->get_start_scn().get_val_for_tx(),
          "end_scn", tables_handle_.get_table(tables_handle_.get_count() - 1)->get_end_scn().get_val_for_tx());
    }
  }
  return ret;
}

int ObTabletMergeCtx::generate_macro_id_list(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  const ObSSTable *new_table = nullptr;
  if (OB_FAIL(merged_table_handle_.get_sstable(new_table))) {
    LOG_WARN("failed to get sstable", K(ret), K(merged_table_handle_));
  } else {
    MEMSET(buf, '\0', buf_len);
    int pret = 0;
    const common::ObIArray<MacroBlockId> &macro_list = new_table->get_meta().get_macro_info().get_data_block_ids();
    int64_t remain_len = buf_len;
    if (macro_list.count() < 40) {
      for (int i = 0; OB_SUCC(ret) && i < macro_list.count(); ++i) {
        if (0 == i) {
          pret = snprintf(buf + strlen(buf), remain_len, "%ld", macro_list.at(i).second_id());
        } else {
          pret = snprintf(buf + strlen(buf), remain_len, ",%ld", macro_list.at(i).second_id());
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

int serialize_medium_list(
    const char *list_name,
    const compaction::ObMediumCompactionInfoList &medium_list,
    ObSSTableMergeInfo &sstable_merge_info,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (medium_list.get_list().get_size() > 0) {
    common::databuff_printf(sstable_merge_info.comment_, sizeof(sstable_merge_info.comment_), pos,
        "%s:{cnt=%d;", list_name, medium_list.get_list().get_size());
    DLIST_FOREACH_NORET(info, medium_list.get_list()) {
      const compaction::ObMediumCompactionInfo *medium_info = static_cast<const compaction::ObMediumCompactionInfo *>(info);
      common::databuff_printf(sstable_merge_info.comment_, sizeof(sstable_merge_info.comment_), pos,
          "%ld,", medium_info->medium_snapshot_);
      if (medium_info->medium_merge_reason_ > ObAdaptiveMergePolicy::AdaptiveMergeReason::NONE) {
        common::databuff_printf(sstable_merge_info.comment_, sizeof(sstable_merge_info.comment_), pos,
            "merge_reason=%s,", ObAdaptiveMergePolicy::merge_reason_to_str(medium_info->medium_merge_reason_));
      }
    }
    if (pos > 0) {
      pos --;
    }
    common::databuff_printf(sstable_merge_info.comment_, sizeof(sstable_merge_info.comment_), pos, "}|");
  }
  return ret;
}

void ObTabletMergeCtx::collect_running_info()
{
  int tmp_ret = OB_SUCCESS;
  ObDagWarningInfo *warning_info = nullptr;
  ObSSTableMergeInfo &sstable_merge_info = merge_info_.get_sstable_merge_info();
  sstable_merge_info.dag_id_ = merge_dag_->get_dag_id();

  // collect medium info dump msg
  int64_t pos = 0;
  serialize_medium_list("new_medium_list", merge_list_, sstable_merge_info, pos);
  serialize_medium_list("serialize_medium_list", tablet_handle_.get_obj()->get_medium_compaction_info_list(), sstable_merge_info, pos);

  ADD_COMPACTION_INFO_PARAM(sstable_merge_info.comment_, sizeof(sstable_merge_info.comment_),
      "time_guard", time_guard_);

  const int64_t dag_key = merge_dag_->hash();
  // calc flush macro speed
  uint32_t exe_ts = time_guard_.get_specified_cost_time(ObCompactionTimeGuard::EXECUTE);
  if (exe_ts > 0 && sstable_merge_info.new_flush_occupy_size_ > 0) {
    sstable_merge_info.new_flush_data_rate_ = (int)(((float)sstable_merge_info.new_flush_occupy_size_/ 1024) / ((float)exe_ts / 1_s));
  }

  if (OB_SUCCESS == share::ObDagWarningHistoryManager::get_instance().get(dag_key, warning_info)) {
    // have failed before
    ADD_COMPACTION_INFO_PARAM(sstable_merge_info.comment_, sizeof(sstable_merge_info.comment_),
        "latest_error_code", warning_info->dag_ret_,
        "latest_error_trace", warning_info->task_id_,
        "retry_cnt", warning_info->retry_cnt_);
  }

  ObScheduleSuspectInfo ret_info;
  int64_t suspect_info_hash = ObScheduleSuspectInfo::gen_hash(MTL_ID(), dag_key);
  if (OB_SUCCESS == ObScheduleSuspectInfoMgr::get_instance().get_suspect_info(suspect_info_hash, ret_info)) {
    ADD_COMPACTION_INFO_PARAM(sstable_merge_info.comment_, sizeof(sstable_merge_info.comment_),
        "add_timestamp", ret_info.add_time_,
        "suspect_schedule_info", ret_info.suspect_info_);
    (void)ObScheduleSuspectInfoMgr::get_instance().del_suspect_info(suspect_info_hash);
  }

  if (OB_TMP_FAIL(MTL(storage::ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(sstable_merge_info))) {
    LOG_WARN_RET(tmp_ret, "failed to add sstable merge info ", K(tmp_ret), K(sstable_merge_info));
  }
}

} // namespace compaction
} // namespace oceanbase
