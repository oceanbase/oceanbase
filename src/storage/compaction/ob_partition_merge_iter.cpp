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
#include "ob_partition_merge_iter.h"
#include "ob_tablet_merge_task.h"
#include "storage/tx_table/ob_tx_table.h"
#include "storage/access/ob_table_read_info.h"
#include "ob_tablet_merge_ctx.h"
#include "storage/column_store/ob_column_oriented_sstable.h"

namespace oceanbase
{
using namespace share::schema;
using namespace share;
using namespace common;
using namespace memtable;
using namespace storage;
using namespace blocksstable;

namespace compaction
{
int ObDefaultRowIter::init(
    const ObMergeParameter &merge_param,
    ObITable *table,
    const ObITableReadInfo *read_info)
{
  UNUSED(read_info);
  int ret = OB_SUCCESS;
  curr_row_count_ = 0;
  const blocksstable::ObDatumRange &query_range = merge_param.merge_rowid_range_;

  if (OB_UNLIKELY(!query_range.is_valid() || table == nullptr || !table->is_co_sstable())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(merge_param));
  } else if (query_range.is_whole_range()) {
    ObSSTable *sstable = static_cast<ObSSTable *>(table);
    total_row_count_ = sstable->get_row_count();
  } else if (OB_UNLIKELY(query_range.start_key_.is_static_rowkey()
                      || query_range.end_key_.is_static_rowkey())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected query range", K(ret), K(query_range));
  } else {
    const int64_t start_rowid = query_range.start_key_.datums_[0].get_int(), end_rowid = query_range.end_key_.datums_[0].get_int();
    total_row_count_ = end_rowid - start_rowid;
    if (query_range.is_left_closed() && query_range.is_right_closed()) {
      total_row_count_++;
    } else if (query_range.is_left_open() && query_range.is_right_open()) {
      total_row_count_--;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(total_row_count_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected total row count", K(ret), K(merge_param), KPC(table));
  } else {
    is_inited_ = true;
  }

  return ret;
}

void ObDefaultRowIter::reset()
{
  total_row_count_ = 0;
  curr_row_count_ = 0;
  ObMergeIter::reset();
}

int ObDefaultRowIter::next()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDefaultRowIter not init", K(ret));
  } else if (curr_row_count_ <= total_row_count_) {
    curr_row_count_++;
  }

  if (OB_SUCC(ret) && curr_row_count_ > total_row_count_) {
    ret = OB_ITER_END;
  }

  return ret;
}

int ObDefaultRowIter::get_curr_row_id(int64_t& row_id) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDefaultRowIter is not inited", K(ret));
  } else if (OB_UNLIKELY(curr_row_count_ <= 0 || curr_row_count_ > total_row_count_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected curr row id", K(ret), K(curr_row_count_), K(total_row_count_));
  } else {
    row_id = curr_row_count_ - 1;
  }
  return ret;
}

const blocksstable::ObDatumRow *ObDefaultRowIter::get_curr_row() const
{
  const blocksstable::ObDatumRow *curr_row = nullptr;

  if (curr_row_count_ > 0 && curr_row_count_ <= total_row_count_) {
    curr_row = &default_row_;
  }

  return curr_row;
}

/*
 *ObPartitionMergeIter
 */
ObPartitionMergeIter::ObPartitionMergeIter(common::ObIAllocator &allocator)
  : tablet_id_(),
    read_info_(nullptr),
    schema_rowkey_column_cnt_(0),
    schema_version_(0),
    merge_range_(),
    table_(nullptr),
    store_ctx_(),
    access_param_(),
    access_context_(),
    row_iter_(nullptr),
    iter_row_count_(0),
    iter_row_id_(-1),
    is_base_iter_(false),
    curr_row_(nullptr),
    iter_end_(false),
    allocator_(allocator),
    last_macro_block_reused_(false),
    is_rowkey_first_row_already_output_(false),
    is_rowkey_shadow_row_reused_(false)
{
}

ObPartitionMergeIter::~ObPartitionMergeIter()
{
  reset();
}

void ObPartitionMergeIter::reset()
{
  if (nullptr != row_iter_) {
    row_iter_->~ObStoreRowIterator();
    row_iter_ = nullptr;
  }
  tablet_id_.reset();
  schema_rowkey_column_cnt_ = 0;
  schema_version_ = 0;
  merge_range_.reset();
  table_ = nullptr;
  access_context_.reset();
  access_param_.reset();
  store_ctx_.reset();
  read_info_ = nullptr;

  iter_row_count_ = 0;
  iter_row_id_ = -1;

  is_base_iter_ = false;
  curr_row_ = nullptr;
  iter_end_ = false;
  is_inited_ = false;
  is_rowkey_first_row_already_output_ = false;
  is_rowkey_shadow_row_reused_ = false;
  ObMergeIter::reset();
}

int ObPartitionMergeIter::init_query_base_params(const ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;
  ObSSTable *sstable = nullptr;
  SCN snapshot_version;
  const ObStaticMergeParam &static_param = merge_param.static_param_;
  if (OB_UNLIKELY(nullptr == read_info_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null read info", K(ret));
  } else if (OB_FAIL(access_param_.init_merge_param(tablet_id_.id(), tablet_id_,
                                                    *read_info_, is_multi_version_merge(static_param.get_merge_type())))) {
    LOG_WARN("Failed to init table access param", K(ret), KPC(this));
  } else if (OB_FAIL(snapshot_version.convert_for_tx(merge_param.merge_version_range_.snapshot_version_))) {
      LOG_WARN("Failed to convert", K(ret), K_(merge_param.merge_version_range_.snapshot_version));
  } else if (OB_FAIL(store_ctx_.init_for_read(static_param.ls_handle_,
                                              INT64_MAX, // query_expire_ts
                                              -1, // lock_timeout_us
                                              snapshot_version))) {
    LOG_WARN("Failed to init store ctx", K(ret), K(static_param), K(snapshot_version));
  } else {
    ObQueryFlag query_flag(ObQueryFlag::Forward,
                           true, /*is daily merge scan*/
                           true, /*is read multiple macro block*/
                           true, /*sys task scan, read one macro block in single io*/
                           false /*full row scan flag, obsoleted*/,
                           false,/*index back*/
                           false); /*query_stat*/
    query_flag.multi_version_minor_merge_ = is_multi_version_merge(static_param.get_merge_type());
    if (OB_FAIL(access_context_.init(query_flag, store_ctx_, allocator_, allocator_,
                                     merge_param.merge_version_range_))) {
      LOG_WARN("Failed to init table access context", K(ret), K(query_flag));
    } else {
      access_context_.trans_state_mgr_ = merge_param.trans_state_mgr_;
      // 1.normal minor merge merge scn equal to end scn
      // 2.backfill may merge scn is bigger than end scn
      access_context_.merge_scn_ = merge_param.merge_scn_;
      if (!static_param.is_backfill_) {
        if (OB_UNLIKELY(access_context_.merge_scn_ != static_param.scn_range_.end_scn_)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "Unexpected merge scn", K(ret), K(merge_param), K(static_param));
        }
      }
    }
  }
  return ret;
}

//only for column store
int ObPartitionMergeIter::init(const ObMergeParameter &merge_param, ObITable *table, const ObITableReadInfo *read_info)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPartitionMergeIter init twice", K(ret));
  } else if (OB_UNLIKELY(!merge_param.is_valid() ||
        read_info == nullptr || table == nullptr || !table->is_major_sstable())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments to init ObPartitionMergeIter", K(ret), K(merge_param), KPC(read_info), KPC(table));
  } else {
    table_ = table;
    read_info_ = read_info;
    is_base_iter_ = true;
    if (OB_FAIL(init(merge_param))) {
      STORAGE_LOG(WARN, "failed to init", K(ret));
    }
  }
  return ret;
}

int ObPartitionMergeIter::init(const ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;
  const ObStaticMergeParam &static_param = merge_param.static_param_;
  tablet_id_ = static_param.get_tablet_id();
  schema_rowkey_column_cnt_ = static_param.schema_->get_rowkey_column_num();
  schema_version_ = static_param.schema_->get_schema_version();
  merge_range_ = table_->is_normal_cg_sstable() ?  merge_param.merge_rowid_range_ : merge_param.merge_range_;
  iter_row_count_ = 0;
  curr_row_ = nullptr;
  iter_end_ = false;
  is_rowkey_first_row_already_output_ = false;
  is_rowkey_shadow_row_reused_ = false;

  if (OB_FAIL(init_query_base_params(merge_param))) {
    LOG_WARN("Failed to init query base params", K(ret));
  } else if (OB_UNLIKELY(!inner_check(merge_param))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to inner init", K(ret), K(*this), K(merge_param));
  } else if (OB_FAIL(inner_init(merge_param))) {
    LOG_WARN("Failed to inner init", K(ret));
  } else {
    is_inited_ = true;
    LOG_DEBUG("Succ to init partition merge iter",  K(*this));
  }

  return ret;
}

int ObPartitionMergeIter::init(const ObMergeParameter &merge_param,
                               const int64_t iter_idx,
                               const ObITableReadInfo *read_info)
{
  int ret = OB_SUCCESS;
  ObITable *table = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPartitionMergeIter init twice", K(ret));
  } else if (OB_UNLIKELY(!merge_param.is_valid()
                         || iter_idx < 0 || nullptr == read_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments to init ObPartitionMergeIter", K(ret),
             K(merge_param), K(iter_idx));
  } else if (FALSE_IT(read_info_ = read_info)) {
  } else if (OB_ISNULL(table = merge_param.get_tables_handle().get_table(iter_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected iter index or tables handle", K(ret), K(iter_idx),
                "tables_handle_count", merge_param.get_tables_handle().get_count(), K(merge_param));
  } else {
    table_ = table;
  }

  if (OB_SUCC(ret)) {
    is_base_iter_ = (iter_idx == 0);
    if (OB_FAIL(init(merge_param))) {
      STORAGE_LOG(WARN, "failed to init", K(ret));
    }
  }

  return ret;
}

int ObPartitionMergeIter::check_merge_range_cross(ObDatumRange &data_range, bool &range_cross)
{
  int ret = OB_SUCCESS;
  range_cross = false;
  if (merge_range_.is_whole_range()) {
    // parallel minor merge should consider open the border macro blocks
  } else {
    int cmp_ret = 0;
    const blocksstable::ObStorageDatumUtils *datum_utils = nullptr;

    if (table_->is_normal_cg_sstable()) {
      const ObITableReadInfo *index_read_info = nullptr;
      if (OB_FAIL(MTL(ObTenantCGReadInfoMgr *)->get_index_read_info(index_read_info))) {
        STORAGE_LOG(WARN, "unexpected null index read info", K(ret));
      } else {
        datum_utils = &index_read_info->get_datum_utils();
      }
    } else {
      datum_utils = &read_info_->get_datum_utils();
    }

    // safe to modify range of curr_macro_block with overwriting ptr only
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(merge_range_.get_start_key().compare(data_range.get_start_key(),
                                                     *datum_utils,
                                                     cmp_ret))) {
      STORAGE_LOG(WARN, "Failed to compare start key", K(ret), K_(merge_range), K(data_range));
    } else if (cmp_ret > 0) {
      data_range.start_key_ = merge_range_.get_start_key();
      range_cross = true;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(merge_range_.get_end_key().compare(data_range.get_end_key(),
                                                          *datum_utils,
                                                          cmp_ret))) {
      STORAGE_LOG(WARN, "Failed to compare end key", K(ret), K_(merge_range), K(data_range));
    } else if (cmp_ret <= 0) {
      data_range.end_key_ = merge_range_.get_end_key();
      range_cross = true;
    }
  }
  LOG_DEBUG("check macro block range cross", K(ret), K(data_range), K(merge_range_), K(range_cross));
  return ret;
}


void ObPartitionMergeIter::revise_macro_range(ObDatumRange &range) const
{
  range.start_key_.datum_cnt_ = MIN(schema_rowkey_column_cnt_, range.start_key_.datum_cnt_);
  range.end_key_.datum_cnt_ = MIN(schema_rowkey_column_cnt_, range.end_key_.datum_cnt_);
}

int ObPartitionMergeIter::get_curr_row_id(int64_t &row_id) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionRowMergeIter is not inited", K(ret), K(*this));
  } else if (OB_ISNULL(curr_row_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected curr row is null", K(ret), KPC(this));
  } else {
    row_id = iter_row_id_;
  }
  return ret;
}

/*
 * ObPartitionRowMergeIter used for major merge
 */

ObPartitionRowMergeIter::ObPartitionRowMergeIter(
    common::ObIAllocator &allocator,
    const bool iter_co_build_row_store,
    const bool &ignore_shadow_row)
  : ObPartitionMergeIter(allocator),
    iter_co_build_row_store_(iter_co_build_row_store),
    ignore_shadow_row_(ignore_shadow_row),
    out_cols_project_()
{
  out_cols_project_.set_allocator(&allocator);
}

ObPartitionRowMergeIter::~ObPartitionRowMergeIter()
{
  reset();
}

bool ObPartitionRowMergeIter::inner_check(const ObMergeParameter &merge_param)
{
  bool bret = true;

  if (!table_->is_sstable()) {
    bret = false;
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "Unexpected table type for major merge", KPC(table_));
  } else if (is_multi_version_merge(merge_param.static_param_.get_merge_type())) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "Unexpected merge type for major merge", K(merge_param));
  }

  return bret;
}

int ObPartitionRowMergeIter::inner_init(const ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;
  iter_row_id_ = -1;
  if (OB_FAIL(inner_init_row_iter(merge_param))) {
    LOG_WARN("Fail to init row iter for table", K(ret));
  } else if (OB_ISNULL(row_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpceted null row iter for table", K(ret), K(*this));
  } else if (ignore_shadow_row_ && OB_FAIL(row_iter_->set_ignore_shadow_row())) {
    STORAGE_LOG(WARN, "fail to set ignore shadow row", K(ret), KPC(row_iter_));
  }

  return ret;
}

int ObPartitionRowMergeIter::construct_out_cols_project(const ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;
  if (iter_co_build_row_store_ && nullptr == access_param_.iter_param_.out_cols_project_) {
    int64_t store_schema_column_cnt = 0;
    int64_t col_index = OB_INVALID_INDEX_INT64;
    const ObStorageSchema *schema = nullptr;
    if (OB_ISNULL(schema = merge_param.get_schema())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("schema is null", K(ret), K(merge_param));
    } else if (OB_FAIL(schema->get_store_column_count(store_schema_column_cnt, true /* full_col, unused */))) {
      LOG_WARN("fail to get store column cnt", KPC(schema));
    } else if (OB_FAIL(out_cols_project_.init(store_schema_column_cnt))) {
      LOG_WARN("fail to init out_cols_project_", K(ret), K(store_schema_column_cnt));
    } else {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < store_schema_column_cnt; idx++) {
        col_index = idx < schema_rowkey_column_cnt_ ? idx : idx + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
        if (OB_FAIL(out_cols_project_.push_back(static_cast<int32_t>(col_index)))) {
          LOG_WARN("fail to push back out cols project", K(ret), K(idx), K(col_index), K_(schema_rowkey_column_cnt), K_(read_info));
        }
      }
      if (OB_SUCC(ret)) {
        access_param_.iter_param_.out_cols_project_ = &out_cols_project_;
        LOG_DEBUG("[RowColSwitch] Construct out cols project", K(out_cols_project_));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K_(iter_co_build_row_store), K(access_param_.iter_param_.out_cols_project_));
  }
  return ret;
}

int ObPartitionRowMergeIter::inner_init_row_iter(const ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;
  if (!table_->is_sstable()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table type", K(ret), KPC(table_));
  } else if (iter_co_build_row_store_) { // only this case scan whole column of co sstable
    if (!table_->is_co_sstable()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid table type", K(ret), K_(iter_co_build_row_store), KPC(table_));
    } else if (OB_FAIL(construct_out_cols_project(merge_param))) {
      LOG_WARN("falied to construct out cols project", K(ret));
    } else if (OB_FAIL(table_->scan(access_param_.iter_param_, access_context_, merge_range_, row_iter_))) {
      LOG_WARN("failed to scan co sstable for merge", K(ret), KPC(table_), K_(merge_range), K_(access_context), K_(access_param));
    }
  } else if (OB_FAIL(static_cast<ObSSTable *>(table_)->ObSSTable::scan(access_param_.iter_param_, access_context_, merge_range_, row_iter_))) {
    LOG_WARN("failed to scan sstable for merge", K(ret), KPC(table_), K_(merge_range), K_(access_context), K_(access_param));
  }
  return ret;
}

int ObPartitionRowMergeIter::next()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionRowMergeIter is not inited", K(ret), K(*this));
  } else if (OB_UNLIKELY(iter_end_)) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(row_iter_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("Unexpected null row iter", K(ret), K(*this));
  } else if (FALSE_IT(curr_row_ = nullptr)) {
  } else if (OB_FAIL(row_iter_->get_next_row(curr_row_))) {
    if (OB_LIKELY(OB_ITER_END == ret)) {
      iter_end_ = true;
    } else {
      LOG_WARN("Failed to get next row from memtable iter", K(ret), K(*this));
    }
  } else {
    iter_row_count_++;
    iter_row_id_++;
    LOG_DEBUG("row iter next row", K(ret), KPC(curr_row_), K(*this));
  }
  return ret;
}

/*
 *ObPartitionMacroMergeIter
 */
ObPartitionMacroMergeIter::ObPartitionMacroMergeIter(common::ObIAllocator &allocator)
  : ObPartitionMergeIter(allocator),
    macro_block_iter_(nullptr),
    curr_block_desc_(),
    curr_block_meta_(),
    cs_datum_range_(),
    macro_block_opened_(false),
    macro_block_opened_for_cmp_(false)
{
  curr_block_desc_.macro_meta_ = &curr_block_meta_;
}

ObPartitionMacroMergeIter::~ObPartitionMacroMergeIter()
{
  reset();
}


void ObPartitionMacroMergeIter::reset()
{
  if (nullptr != macro_block_iter_) {
    macro_block_iter_->~ObIMacroBlockIterator();
    allocator_.free(macro_block_iter_);
    macro_block_iter_ = nullptr;
  }
  curr_block_desc_.reset();
  curr_block_meta_.reset();
  curr_block_desc_.macro_meta_ = &curr_block_meta_;
  macro_block_opened_ = false;
  ObPartitionMergeIter::reset();
}

bool ObPartitionMacroMergeIter::inner_check(const ObMergeParameter &merge_param)
{
  bool bret = true;
  const ObStaticMergeParam &static_param = merge_param.static_param_;
  if (OB_UNLIKELY(!is_major_or_meta_merge_type(static_param.get_merge_type()))) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "Unexpected merge type for major macro merge iter", K(bret), K(static_param));
  } else if (static_param.is_full_merge_) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "Unexpected full merge for major macro merge iter", K(bret), K(static_param));
  } else if (OB_UNLIKELY(!table_->is_major_sstable())) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "Unexpected base table type for major macro merge iter", K(bret), KPC(table_));
  }
  return bret;
}

int ObPartitionMacroMergeIter::next_range()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(iter_end_)) {
    ret = OB_ITER_END;
  } else if (FALSE_IT(reset_macro_block_desc())) {
  } else if (OB_SUCC(macro_block_iter_->get_next_macro_block(curr_block_desc_))) {
    macro_block_opened_for_cmp_ = false;
    macro_block_opened_ = false;
    bool need_open = false;

    if (OB_FAIL(check_merge_range_cross(curr_block_desc_.range_, need_open))) {
      LOG_WARN("failed to check range cross", K(ret), K(curr_block_desc_.range_));
    } else if (need_open) {
      if (OB_FAIL(open_curr_range(false/*for rewrite*/))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to open curr range", K(ret), K(curr_block_desc_));
        }
      } else {
        LOG_TRACE("open macro for cross range", K(ret), K(curr_block_desc_), KPC(table_), KPC(this));
      }
    }
  } else if (OB_UNLIKELY(OB_ITER_END != ret)) {
    LOG_WARN("Failed to get next macro block", K(ret));
  } else {
    iter_end_ = true;
  }

  return ret;
}

int ObPartitionMacroMergeIter::open_curr_range(const bool for_rewrite, const bool for_compare)
{
  int ret = OB_SUCCESS;

  UNUSEDx(for_rewrite, for_compare);
  if (OB_UNLIKELY(macro_block_opened_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("Unepxcted opened macro block to open", K(ret));
  } else {
    ObSSTableRowWholeScanner *iter = reinterpret_cast<ObSSTableRowWholeScanner *>(row_iter_);
    if (macro_block_opened_for_cmp_) {
      if (OB_FAIL(iter->switch_query_range(merge_range_))) {
        LOG_WARN("fail to switch_query_range", K(ret));
      }
    } else {
      iter->reuse();
      if (OB_FAIL(iter->open(
          access_param_.iter_param_,
          access_context_,
          merge_range_,
          curr_block_desc_,
          *reinterpret_cast<ObSSTable *>(table_)))) {
        LOG_WARN("fail to open iter", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      macro_block_opened_ = true;
      macro_block_opened_for_cmp_ = false;
      ret = next();
    }
  }

  return ret;
}

int ObPartitionMacroMergeIter::inner_init(const ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;

  ObSSTable *sstable = static_cast<ObSSTable *>(table_);
  const storage::ObITableReadInfo *index_read_info = nullptr;
  if (table_->is_normal_cg_sstable()) {
    if (OB_FAIL(MTL(ObTenantCGReadInfoMgr *)->get_index_read_info(index_read_info))) {
      SERVER_LOG(WARN, "failed to get index read info from ObTenantCGReadInfoMgr", KR(ret));
    }
  } else {
    index_read_info =  merge_param.static_param_.rowkey_read_info_;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(index_read_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null index read info", K(ret));
  } else if (OB_FAIL(sstable->scan_macro_block(
      merge_range_, *index_read_info, allocator_, macro_block_iter_, false, true, true))) {
    LOG_WARN("Fail to scan macro block", K(ret), KPC(sstable), K(merge_range_));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObSSTableRowWholeScanner)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc memory for minor merge row scanner", K(ret));
  } else if (FALSE_IT(row_iter_ = new (buf) ObSSTableRowWholeScanner())) {
  } else {
    macro_block_opened_ = false;
  }

  return ret;
}

int ObPartitionMacroMergeIter::next()
{
  int ret = OB_SUCCESS;
  uint64_t reuse_row_count = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionMacroMergeIter is not inited", K(ret), K(*this));
  } else if (OB_UNLIKELY(iter_end_)) {
    ret = OB_ITER_END;
  } else if (FALSE_IT(curr_row_ = nullptr)) {
  } else if (macro_block_opened_ && OB_SUCC(row_iter_->get_next_row(curr_row_))) {
    iter_row_count_++;
    iter_row_id_++;
  } else if (OB_UNLIKELY(OB_SUCCESS != ret && OB_ITER_END != ret)) {
    LOG_WARN("Failed to get next row", K(ret), K(*this));
  } else {
    reuse_row_count = macro_block_opened_ ? 0 : curr_block_desc_.row_count_;
    if (OB_FAIL(next_range())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Failed to get next range", K(ret), K(*this));
      }
    }
    if (OB_ITER_END == ret || OB_SUCCESS == ret) {
      iter_row_id_ += reuse_row_count;
    }
  }
  return ret;
}

int ObPartitionMacroMergeIter::get_curr_range_end_rowid(int64_t &row_id) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionMacroMergeIter is not inited", K(ret));
  } else if (macro_block_opened_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected opened macro block to get range row count", K(ret), K(curr_block_desc_), K(OB_ITER_END));
  } else {
    row_id = curr_block_desc_.row_count_ + get_last_row_id();
  }
  return ret;
}

int ObPartitionMacroMergeIter::get_curr_range(ObDatumRange &range) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionMacroMergeIter is not inited", K(ret));
  } else if (macro_block_opened_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected opened macro block to get range", K(ret), K(curr_block_desc_), K(OB_ITER_END));
  } else {
    range = curr_block_desc_.range_;
    revise_macro_range(range);
  }
  return ret;
}

int ObPartitionMacroMergeIter::need_open_curr_range(const blocksstable::ObDatumRow &row, bool &need_open, const int64_t row_id_for_cg)
{
  int ret = OB_SUCCESS;
  need_open = true;
  if (OB_UNLIKELY(get_curr_row() != nullptr || table_ == nullptr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected macro_block_opened or table", K(ret), K(macro_block_opened_), KPC(table_));
  } else if (row.row_flag_.is_delete()) {
    if (OB_FAIL(exist(row, need_open))) {
      STORAGE_LOG(WARN, "fail to check exist", K(ret));
    }
  } else {
    if (OB_FAIL(check_row_changed(row, row_id_for_cg, need_open))) {
      STORAGE_LOG(WARN, "fail to check row chenged", K(ret));
    }
  }
  return ret;
}

int ObPartitionMacroMergeIter::exist(const ObDatumRow &row, bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObDatumRowkey rowkey;
  const blocksstable::ObDatumRow *temp_row = nullptr;
  blocksstable::ObDatumRange query_range;
  query_range.end_key_.set_max_rowkey();
  query_range.set_left_closed();
  ObSSTableRowWholeScanner *iter = reinterpret_cast<ObSSTableRowWholeScanner *>(row_iter_);

  if (OB_FAIL(query_range.start_key_.assign(row.storage_datums_, schema_rowkey_column_cnt_))) {
    STORAGE_LOG(WARN, "Failed to assign rowkey", K(ret), K(row), K_(schema_rowkey_column_cnt));
  } else if (macro_block_opened_for_cmp_) {
    if (OB_FAIL(iter->switch_query_range(query_range))) {
      LOG_WARN("fail to switch_query_range", K(ret), K(cs_datum_range_));
    }
  } else {
    iter->reuse();
    if (OB_FAIL(iter->open(
              access_param_.iter_param_,
              access_context_,
              query_range,
              curr_block_desc_,
              *static_cast<ObSSTable *>(table_)))) {
        LOG_WARN("fail to open iter", K(ret), K(cs_datum_range_));
    } else {
      macro_block_opened_for_cmp_ = true;
    }
  }


  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(iter->get_next_row(temp_row))) {
    STORAGE_LOG(WARN, "fail to get next row", K(ret), KPC(iter));
  } else if (OB_ISNULL(temp_row)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unecxpected null row", K(ret));
  } else if (OB_FAIL(rowkey.assign(temp_row->storage_datums_, schema_rowkey_column_cnt_))) {
    STORAGE_LOG(WARN, "Failed to assign rowkey", K(ret), KPC(temp_row), K_(schema_rowkey_column_cnt));
  } else {
    int temp_cmp_ret = 0;
    if (OB_FAIL(query_range.start_key_.compare(rowkey, read_info_->get_datum_utils(), temp_cmp_ret))) {
      STORAGE_LOG(WARN, "Failed to compare rowkey", K(ret), K(rowkey), K(query_range.start_key_), K(read_info_));
    } else if (temp_cmp_ret == 0) {
      is_exist = true;
    } else {
      is_exist = false;
    }
  }
  iter->reset_query_range();
  return ret;
}

int ObPartitionMacroMergeIter::check_row_changed(const blocksstable::ObDatumRow &row, const int64_t row_id, bool &is_changed)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!table_->is_normal_cg_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected macro_block_opened or table", K(ret), KPC(table_));
  } else {
    ObSSTableRowWholeScanner *iter = reinterpret_cast<ObSSTableRowWholeScanner *>(row_iter_);
    int64_t start_row_id = 0;
    const blocksstable::ObDatumRow *temp_row = nullptr;
    if (merge_range_.get_start_key().is_min_rowkey()) {
       start_row_id = row_id;
    } else {
      const int64_t start_idx = merge_range_.get_start_key().datums_[0].get_int();
      const int64_t offset = merge_range_.is_left_open() ? 1 : 0;
      start_row_id = start_idx + offset + row_id;
    }

    cs_datum_range_.set_datum_range(start_row_id, start_row_id + curr_block_desc_.row_count_);
    if (macro_block_opened_for_cmp_) {
      if (OB_FAIL(iter->switch_query_range(cs_datum_range_.get_cs_datum_range()))) {
        LOG_WARN("fail to switch_query_range", K(ret), K(cs_datum_range_));
      }
    } else {
      iter->reuse();
      if (OB_FAIL(iter->open(
          access_param_.iter_param_,
          access_context_,
          cs_datum_range_.get_cs_datum_range(),
          curr_block_desc_,
          *reinterpret_cast<ObSSTable *>(table_)))) {
        LOG_WARN("fail to open iter", K(ret), K(cs_datum_range_));
      } else {
        macro_block_opened_for_cmp_ = true;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(iter->get_next_row(temp_row))) {
      STORAGE_LOG(WARN, "fail to get next row", K(ret), KPC(iter), K(row_id));
    } else if (OB_FAIL(row.is_datums_changed(*temp_row, is_changed))) {
      STORAGE_LOG(WARN, "fail to check datums equal", K(ret), KPC(temp_row), K(row));
    }
  }
  return ret;
}

/*
 *ObPartitionMicroMergeIter
 */
ObPartitionMicroMergeIter::ObPartitionMicroMergeIter(common::ObIAllocator &allocator)
  : ObPartitionMacroMergeIter(allocator),
    micro_block_iter_(),
    micro_row_scanner_(nullptr),
    curr_micro_block_(nullptr),
    micro_block_opened_(false),
    macro_reader_(),
    need_reuse_micro_block_(true)
{
}

ObPartitionMicroMergeIter::~ObPartitionMicroMergeIter()
{
  reset();
}

void ObPartitionMicroMergeIter::reset()
{
  micro_block_iter_.reset();
  if (OB_NOT_NULL(micro_row_scanner_)) {
    micro_row_scanner_->~ObIMicroBlockRowScanner();
    micro_row_scanner_ = nullptr;
  }
  curr_micro_block_ = nullptr;
  micro_block_opened_ = false;
  need_reuse_micro_block_ = true;
  ObPartitionMacroMergeIter::reset();
}

bool ObPartitionMicroMergeIter::inner_check(const ObMergeParameter &merge_param)
{
  bool bret = true;
  const ObStaticMergeParam &static_param = merge_param.static_param_;
  if (OB_UNLIKELY(!is_major_or_meta_merge_type(static_param.get_merge_type()))) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "Unexpected merge type for major micro merge iter", K(bret), K(merge_param));
  } else if (OB_UNLIKELY(static_param.merge_level_ != MICRO_BLOCK_MERGE_LEVEL)) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "Unexpected merge level for major micro merge iter", K(bret), K(merge_param));
  } else if (OB_UNLIKELY(static_param.is_full_merge_)) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "Unexpected full merge for major micro merge iter", K(bret), K(merge_param));
  } else if (OB_UNLIKELY(!is_base_iter())) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "Unexpected iter idx for major micro merge iter", K(bret), K(merge_param));
  } else if (OB_UNLIKELY(!table_->is_major_sstable())) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "Unexpected base table type for major macro merge iter", K(bret), KPC(table_));
  }

  return bret;
}


int ObPartitionMicroMergeIter::inner_init(const ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;

  if (OB_FAIL(ObPartitionMacroMergeIter::inner_init(merge_param))) {
    STORAGE_LOG(WARN, "Failed to do macro merge iter init", K(ret), K(merge_param));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMicroBlockRowScanner)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc memory for multi version micro block scanner", K(ret));
  } else if (FALSE_IT(micro_row_scanner_ = new (buf) ObMicroBlockRowScanner(allocator_))) {
  } else if (OB_FAIL(micro_row_scanner_->init(access_param_.iter_param_,
                                              access_context_,
                                              reinterpret_cast<ObSSTable *>(table_)))) {
    LOG_WARN("Failed to init micro row scanner", K(ret), K(access_param_), K(access_context_));
  } else {
    curr_micro_block_ = nullptr;
    micro_block_opened_ = false;
  }

  return ret;
}

// check before open each macro block
void ObPartitionMicroMergeIter::check_need_reuse_micro_block()
{
  if (curr_block_desc_.schema_version_ <= 0 || curr_block_desc_.schema_version_ != schema_version_) {
    need_reuse_micro_block_ = false;
  } else {
    need_reuse_micro_block_ = true;
  }
}

int ObPartitionMicroMergeIter::next_range()
{
  int ret = OB_SUCCESS;
  uint64_t old_micro_row_count = curr_micro_block_ != nullptr && micro_block_opened_ == false ?
                                  curr_micro_block_->header_.row_count_ : 0;
  curr_micro_block_ = nullptr;
  if (OB_NOT_NULL(micro_row_scanner_)) {
    micro_row_scanner_->reuse();
  }

  if (OB_UNLIKELY(iter_end_)) {
    ret = OB_ITER_END;
  } else if (macro_block_opened_) {
    // try get next micro block
    if (need_reuse_micro_block_) {
      micro_block_opened_ = false;
      if (OB_SUCC(micro_block_iter_.next(curr_micro_block_))) {
      } else if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Failed to get next micro block", K(ret));
      } else {
        // macro block iter end, close the macro block
        macro_block_opened_ = false;
        ret = OB_SUCCESS;
      }
      iter_row_id_ += old_micro_row_count;
    } else {
      macro_block_opened_ = false;
    }
  } else {
    iter_row_id_ += curr_block_desc_.row_count_;
  }

  if (OB_FAIL(ret)) {
  } else if (macro_block_opened_) {
    // no need open current macro block
  } else if (FALSE_IT(micro_block_iter_.reset())) {
  } else if (FALSE_IT(reset_macro_block_desc())) {
  } else if (OB_SUCC(macro_block_iter_->get_next_macro_block(curr_block_desc_))) {
    check_need_reuse_micro_block();
    macro_block_opened_for_cmp_ = false;
    macro_block_opened_ = false;
    micro_block_opened_ = false;
  } else if (OB_UNLIKELY(OB_ITER_END != ret)) {
    LOG_WARN("Failed to get next macro block", K(ret), KPC(macro_block_iter_));
  } else {
    iter_end_ = true;
  }
  return ret;
}


int ObPartitionMicroMergeIter::open_curr_range(const bool for_rewrite, const bool for_compare)
{
  int ret = OB_SUCCESS;
  UNUSED(for_compare);
  const ObITableReadInfo *read_info = nullptr;
  if (OB_UNLIKELY(micro_block_opened_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("Unexpected opened micro block to open", K(ret), K(*this));
  } else if (for_rewrite || !need_reuse_micro_block_) {
    if (macro_block_opened_) {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("Unexpected opened macro block to open", K(ret), K(*this));
    } else {
      micro_block_opened_ = false;
      need_reuse_micro_block_ = false;
      ret = ObPartitionMacroMergeIter::open_curr_range(for_rewrite);
      LOG_DEBUG("open curr range for macro block", K(*this), K(curr_block_desc_));
    }
  } else if (macro_block_opened_) {
    if (OB_FAIL(open_curr_micro_block())) {
      STORAGE_LOG(ERROR, "Failed to open curr micro block", K(ret), K(for_rewrite), K(*this));
    } else {
      LOG_DEBUG("open curr range for micro block", K(*this));
    }
  } else {
    if (table_->is_normal_cg_sstable()) {
      if (OB_FAIL(MTL(ObTenantCGReadInfoMgr *)->get_index_read_info(read_info))) {
        LOG_WARN("failed to get index read info from ObTenantCGReadInfoMgr", K(ret));
      }
    } else {
      read_info = read_info_;
    }
    micro_block_iter_.reset();
    if (FAILEDx(micro_block_iter_.init(
                curr_block_desc_,
                *read_info,
                macro_block_iter_->get_micro_index_infos(),
                macro_block_iter_->get_micro_endkeys(),
                static_cast<ObRowStoreType>(curr_block_desc_.row_store_type_),
                reinterpret_cast<ObSSTable *>(table_)))) {
      LOG_WARN("Failed to init micro_block_iter", K(ret), K_(curr_block_desc));
    } else {
      micro_block_opened_ = false;
      macro_block_opened_ = true;
      micro_row_scanner_->reuse();
      ret = next();
      LOG_DEBUG("init micro block iter for macro block", K(*this), K(macro_block_iter_->get_micro_endkeys()));
    }
  }

  return ret;
}

int ObPartitionMicroMergeIter::check_row_changed(const blocksstable::ObDatumRow &row, const int64_t row_id, bool &is_changed)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(micro_block_opened_ || !table_->is_normal_cg_sstable() || row_id <= iter_row_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected arguments", K(ret), K(micro_block_opened_), KPC(table_), K(row_id), K(iter_row_id_));
  } else if (macro_block_opened_) {
    const blocksstable::ObDatumRow *temp_row = nullptr;
    if (OB_FAIL(open_curr_micro_block(row_id - iter_row_id_ - 1))) {
      STORAGE_LOG(WARN, "fail to open curr micro", K(ret), K(row_id), K(iter_row_id_));
    } else if (OB_FAIL(micro_row_scanner_->get_next_row(temp_row))) {
      STORAGE_LOG(WARN, "fail to get next row", K(ret), KPC(micro_row_scanner_));
    } else if (OB_ISNULL(temp_row)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected temp row", K(ret), KPC(micro_row_scanner_));
    } else if (OB_FAIL(row.is_datums_changed(*temp_row, is_changed))) {
      STORAGE_LOG(WARN, "fail to check datums equal", K(ret), KPC(temp_row), K(row));
    }
  } else if (OB_FAIL(ObPartitionMacroMergeIter::check_row_changed(row, row_id, is_changed))) {
    STORAGE_LOG(WARN, "fail to check_row_changed", K(ret));
  }

  return ret;
}

int ObPartitionMicroMergeIter::open_curr_micro_block(const int64_t start_row_id)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData decompressed_data;
  const ObMicroIndexInfo *micro_index_info = curr_micro_block_->micro_index_info_;
  ObMicroBlockDesMeta micro_des_meta;
  bool is_compressed = false;
  if (OB_UNLIKELY(!macro_block_opened_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("Unexpected closed macro block to open micro block", K(ret));
  } else if (OB_ISNULL(micro_row_scanner_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("Unexpected null micro row scanner", K(ret));
  } else if (FALSE_IT(micro_row_scanner_->reuse())) {
  } else if (OB_UNLIKELY(!curr_micro_block_->range_.is_valid())
      || OB_ISNULL(micro_index_info)
      || OB_UNLIKELY(!micro_index_info->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected micro block", K(ret), KPC(curr_micro_block_));
  } else if (OB_FAIL(micro_index_info->row_header_->fill_micro_des_meta(false, micro_des_meta))) {
    LOG_WARN("Fail to fill micro block deserialize meta", K(ret), KPC(micro_index_info));
  } else if (OB_FAIL(macro_reader_.decrypt_and_decompress_data(
      micro_des_meta,
      curr_micro_block_->data_.get_buf(),
      curr_micro_block_->data_.get_buf_size(),
      decompressed_data.get_buf(),
      decompressed_data.get_buf_size(),
      is_compressed))) {
    LOG_WARN("Failed to decrypt and decompress data", K(ret), KPC_(curr_micro_block));
  } else if (table_->is_normal_cg_sstable()) {
    ObCSRange range;
    range.start_row_id_ = MAX(0, start_row_id);
    range.end_row_id_ = curr_micro_block_->header_.row_count_ - 1;
    if (OB_FAIL(micro_row_scanner_->open_column_block(curr_block_desc_.macro_block_id_,
                                          decompressed_data,
                                          range))) {
      STORAGE_LOG(WARN, "failed to open column block", K(ret), K(curr_block_desc_), K(decompressed_data),K(range));
    }
  } else if (OB_FAIL(micro_row_scanner_->set_range(curr_micro_block_->range_))) {
    LOG_WARN("Failed to init micro scanner", K(ret));
  } else if (OB_FAIL(micro_row_scanner_->open(
      curr_block_desc_.macro_block_id_,
      decompressed_data,
      micro_block_iter_.is_left_border(),
      micro_block_iter_.is_right_border()))) {
    LOG_WARN("Failed to open micro scanner", K(ret));
  }

  if (OB_SUCC(ret) && start_row_id == -1) {
    micro_block_opened_ = true;
    ret = next();
  }

  return ret;
}

int ObPartitionMicroMergeIter::next()
{
  int ret = OB_SUCCESS;
  bool row_itered = false;
  bool range_cross = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionMacroMergeIter is not inited", K(ret), K(*this));
  } else if (OB_UNLIKELY(iter_end_)) {
    ret = OB_ITER_END;
  } else {
    curr_row_ = nullptr;
    if (!macro_block_opened_) {
    } else if (!need_reuse_micro_block_) {
      // macro block opened and use macro block row iter
      if (OB_SUCC(row_iter_->get_next_row(curr_row_))) {
        row_itered = true;
      }
      LOG_DEBUG("Merge iter next with macro iter", K(*this));
    } else if (micro_block_opened_) {
      // micor block opened
      if (OB_SUCC(micro_row_scanner_->get_next_row(curr_row_))) {
        row_itered = true;
      }
      LOG_DEBUG("Merge iter next with micro iter", K(*this));
    }

    if (OB_SUCC(ret) && row_itered) {
      // row iter get next row
      iter_row_count_++;
      iter_row_id_++;
    } else if (OB_UNLIKELY(OB_SUCCESS != ret && OB_ITER_END != ret)) {
      LOG_WARN("Failed to get next row", K(ret), K(*this));
    } else if (OB_FAIL(next_range())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Failed to get next range", K(ret), K(*this));
      }
    } else if (!macro_block_opened_
        && OB_FAIL(check_merge_range_cross(curr_block_desc_.range_, range_cross))) {
      LOG_WARN("failed to check range cross", K(ret), K(curr_block_desc_.range_));
    } else if (range_cross) {
      need_reuse_micro_block_ = false;
      if (OB_FAIL(open_curr_range(false/*for rewrite*/))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to open curr range", K(ret), K(curr_block_desc_));
        }
      } else {
        LOG_TRACE("open macro for cross range", K(ret), K(curr_block_desc_), KPC(table_), KPC(curr_row_));
      }
    } else {
      LOG_DEBUG("Merge iter next with range", K(*this));
    }
  }

  return ret;
}

int ObPartitionMicroMergeIter::get_curr_range_end_rowid(int64_t &row_id) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionMacroMergeIter is not inited", K(ret));
  } else if (macro_block_opened_) {
    if (OB_ISNULL(curr_micro_block_) || micro_block_opened_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null micro block", K(ret), KPC(curr_micro_block_), K(micro_block_opened_));
    } else {
      row_id = curr_micro_block_->header_.row_count_ + get_last_row_id();
    }
  } else {
    row_id = curr_block_desc_.row_count_ + get_last_row_id();
  }

  return ret;
}

int ObPartitionMicroMergeIter::get_curr_range(ObDatumRange &range) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionMacroMergeIter is not inited", K(ret));
  } else if (macro_block_opened_) {
    if (OB_ISNULL(curr_micro_block_) || micro_block_opened_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null micro block", K(ret), K(*this));
    } else {
      range = curr_micro_block_->range_;
    }
  } else {
    range = curr_block_desc_.range_;
  }
  if (OB_SUCC(ret)) {
    revise_macro_range(range);
  }
  return ret;
}

/*
 *ObPartitionMinorRowMergeIter
 */
ObPartitionMinorRowMergeIter::ObPartitionMinorRowMergeIter(common::ObIAllocator &allocator)
  : ObPartitionMergeIter(allocator),
    obj_copy_allocator_("MinorMergeObj", OB_MALLOC_MIDDLE_BLOCK_SIZE, MTL_ID(), ObCtxIds::MERGE_NORMAL_CTX_ID),
    nop_pos_(),
    row_queue_(),
    check_committing_trans_compacted_(true),
    ghost_row_count_(0)
{
  for (int i = 0; i < CRI_MAX; ++i) {
    nop_pos_[i] = nullptr;
  }
  if (share::is_reserve_mode()) {
    obj_copy_allocator_.set_ctx_id(ObCtxIds::MERGE_RESERVE_CTX_ID);
  }
}

ObPartitionMinorRowMergeIter::~ObPartitionMinorRowMergeIter()
{
  reset();
}

void ObPartitionMinorRowMergeIter::reset()
{
  for (int i = 0; i < CRI_MAX; ++i) {
    if (nullptr != nop_pos_[i]) {
      nop_pos_[i]->~ObNopPos();
      nop_pos_[i] = nullptr;
    }
  }
  row_queue_.reset();
  obj_copy_allocator_.reset();
  check_committing_trans_compacted_ = true;
  ghost_row_count_ = 0;
  ObPartitionMergeIter::reset();
}

bool ObPartitionMinorRowMergeIter::inner_check(const ObMergeParameter &merge_param)
{
  bool bret = true;
  const ObStaticMergeParam &static_param = merge_param.static_param_;
  if (OB_UNLIKELY(!is_multi_version_merge(static_param.get_merge_type()) && !compaction::is_backfill_tx_merge(static_param.get_merge_type()))) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "Unexpected merge type for minor row merge iter", K(bret), K(merge_param));
  } else if (OB_UNLIKELY(static_param.merge_level_ != MACRO_BLOCK_MERGE_LEVEL)) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "Unexpected merge level for minor row merge iter", K(bret), K(merge_param));
  } else if (OB_UNLIKELY(!table_->is_multi_version_table())) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "Unexpected table type for minor row merge iter", K(bret), KPC(table_));
  }

  return bret;
}


int ObPartitionMinorRowMergeIter::common_minor_inner_init(const ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;
  int64_t row_column_cnt = 0;
  void *buf = nullptr;
  check_committing_trans_compacted_ = true;
  if (OB_FAIL(merge_param.get_schema()->get_stored_column_count_in_sstable(row_column_cnt))) {
    LOG_WARN("Failed to get full store column count", K(ret));
  } else if (OB_FAIL(row_queue_.init(row_column_cnt))) {
    LOG_WARN("failed to init row_queue", K(ret), K(row_column_cnt));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObNopPos) * CRI_MAX))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "Failed to alloc memory for noppos", K(ret));
  } else { // read flat row
    char *buf_pos = (char *)buf;
    for (int i = 0; OB_SUCC(ret) && i < CRI_MAX; ++i) { // init nop pos
      nop_pos_[i] = new (buf_pos) ObNopPos();
      if (OB_FAIL(nop_pos_[i]->init(allocator_, OB_ROW_MAX_COLUMNS_COUNT))) {
        LOG_WARN("failed to init first row nop pos", K(ret));
      } else {
        buf_pos += sizeof(ObNopPos);
      }
    } // end of for
  }
  return ret;
}

int ObPartitionMinorRowMergeIter::inner_init(const ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(common_minor_inner_init(merge_param))) {
    LOG_WARN("Failed to do commont minor inner init", K(ret), K(merge_param));
  } else if (OB_UNLIKELY(NULL == table_
      || (table_->is_data_memtable() && !is_mini_merge(merge_param.static_param_.get_merge_type())))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected memtable for mini minor merge", K(ret), K(merge_param), KPC(table_));
  } else if (OB_FAIL(table_->scan(access_param_.iter_param_, access_context_,
                                  merge_range_, row_iter_))) {
    LOG_WARN("Fail to init row iter for table", K(ret), KPC(table_),
             K_(merge_range), K_(access_context), K_(access_param));
  } else if (OB_ISNULL(row_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpceted null row iter for sstable", K(ret), K(*this));
  }
  return ret;
}

int ObPartitionMinorRowMergeIter::inner_next(const bool open_macro)
{
  int ret = OB_SUCCESS;

  UNUSED(open_macro);
  if (OB_FAIL(row_iter_->get_next_row(curr_row_))) {
    if (OB_LIKELY(OB_ITER_END == ret)) {
      iter_end_ = true;
    } else {
      LOG_WARN("Failed to get next row from iter", K(ret), K(*this));
    }
  } else {
    iter_row_count_++;
    LOG_DEBUG("row iter next row", K(ret), KPC(curr_row_), K(*this));
  }

  return ret;
}


int ObPartitionMinorRowMergeIter::compact_border_row(const bool last_row)
{
  int ret = OB_SUCCESS;
  ObNopPos *nop_pos = nullptr;
  const int64_t  nop_idx = last_row ? CRI_LAST_ROW : CRI_FIRST_ROW;

  if (OB_ISNULL(curr_row_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("Unexpceted current row", K(ret));
  } else if (curr_row_->is_ghost_row()) {
    // ghost row no need to compact
  } else if (OB_ISNULL(nop_pos = nop_pos_[nop_idx])) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("Unexpected null first row", K(ret), K(nop_idx));
  } else {
    bool final_result = false;
    ObDatumRow *border_row = last_row ? row_queue_.get_last() : row_queue_.get_first();
    if (OB_ISNULL(border_row)) {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("Unexpected null border row", K(ret), K(last_row), K_(row_queue));
    } else if (border_row->is_compacted_multi_version_row()) {
      // border row has already been compacted
    } else if (OB_FAIL(storage::ObRowFuse::fuse_row(*curr_row_, *border_row, *nop_pos, final_result,
                                                    &obj_copy_allocator_))) {
      STORAGE_LOG(WARN, "Failed to fuse row", K(ret));
    } else if (final_result) {
      border_row->set_compacted_multi_version_row();
    }
    LOG_DEBUG("try to compact border row", K(ret), K(last_row), KPC(curr_row_), KPC(border_row));
    if (!last_row) { // fuse flag to first row
      border_row->row_flag_.fuse_flag(curr_row_->row_flag_);
    }
  }

  return ret;
}

int ObPartitionMinorRowMergeIter::check_meet_another_trans()
{
  int ret = OB_SUCCESS;
  int64_t trans_version_idx = schema_rowkey_column_cnt_;
  ObDatumRow *last_row = row_queue_.get_last();

  if (row_queue_.count() <= 0) {
  } else if (OB_UNLIKELY(nullptr == curr_row_ || nullptr == last_row)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("Unexpected null curr row or last row", K(ret), KP(curr_row_), KP(last_row));
  } else if (curr_row_->is_ghost_row()) {
    // ghost row is a virtual last row
  } else if (curr_row_->storage_datums_[trans_version_idx].get_int() !=
             last_row->storage_datums_[trans_version_idx].get_int()) {
    ObDatumRow *first_row = row_queue_.get_first();
    if (!first_row->is_shadow_row()) {
      if (OB_UNLIKELY(1 != row_queue_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected row queue", K(ret), K(row_queue_.count()), KPC(row_queue_.get_first()), KPC(this));
      } else if (OB_FAIL(row_queue_.add_row(*first_row, obj_copy_allocator_))) {
        LOG_WARN("failed to add row queue", K(ret), KPC(first_row), K(row_queue_));
      } else if (OB_FAIL(ObShadowRowUtil::make_shadow_row(schema_rowkey_column_cnt_ + 1/*sql_sequence_col_idx*/, *first_row))) {
        LOG_WARN("failed to make shadow row", K(ret), KPC(first_row), K_(schema_rowkey_column_cnt));
      }
    }

    if (OB_SUCC(ret) && !curr_row_->is_shadow_row()) {
      if (OB_FAIL(row_queue_.add_empty_row(obj_copy_allocator_))) {
        LOG_WARN("Failed to add empty row into row queue", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionMinorRowMergeIter::compact_old_row()
{
  int ret = OB_SUCCESS;
  row_queue_.reuse();
  obj_copy_allocator_.reuse();
  if (OB_FAIL(row_queue_.add_empty_row(obj_copy_allocator_))) {
    LOG_WARN("Failed to add empty row into row queue", K(ret));
  }
  while(OB_SUCC(ret)) {
    if (curr_row_->is_shadow_row()) {
    } else if (OB_FAIL(compact_border_row(false/*last_row*/))) {
      LOG_WARN("Failed to compact first row", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (curr_row_->is_last_multi_version_row()) { // meet L flag
      row_queue_.get_last()->set_last_multi_version_row();
      if (OB_FAIL(row_queue_.get_next_row(curr_row_))) {
        LOG_WARN("Failed to get next row from row_queue", K(ret));
      }
      break;
    } else if (OB_FAIL(inner_next(true /*open_macro*/))) {
      LOG_WARN("Failed to inner next for compact first row", K(ret));
    }
  } // end of while
  return ret;
}

int ObPartitionMinorRowMergeIter::check_compact_finish(bool &finish)
{
  int ret = OB_SUCCESS;
  ObDatumRow *first_row = row_queue_.get_first();
  ObDatumRow *last_row = row_queue_.get_last();
  finish = false;

  if (OB_UNLIKELY(nullptr == first_row || nullptr == last_row || nullptr == curr_row_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("Unexpected null curr row or last row", K(ret), KP(curr_row_), KP(last_row),
                KP(first_row));
  } else {
    if (curr_row_->is_last_multi_version_row()) {
      last_row->set_last_multi_version_row();
      finish = true;
    }
    if (first_row->is_shadow_row()) {
      finish = true;
    }
  }

  return ret;
}

// first row is not compacted means that the first row is a uncommited row originly
// we need compact all the rows with same rowkey within the transaction across macro block
int ObPartitionMinorRowMergeIter::try_make_committing_trans_compacted()
{
  int ret = OB_SUCCESS;
  bool compact_finish = false;

  if (OB_ISNULL(curr_row_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("Unexpceted null current row", K(ret), KP_(curr_row));
  } else if (curr_row_->is_uncommitted_row() || curr_row_->is_ghost_row()) {
    // skip uncommited row and last row(including ghost row)
  } else if (check_committing_trans_compacted_) {
    // need check first output row is compacted
    check_committing_trans_compacted_ = false;
    if (is_compact_completed_row() && !is_curr_row_commiting()) {
      // 1. if first row is already compacted, all the rows following are all from commited transaction;
      // 2. and if first row is last row from commited transaction, this row should contain last flag;
      // 3. the ouput row from committing transaction without last flag could not have compact flag
    } else {
      row_queue_.reuse();
      obj_copy_allocator_.reuse();
      if (OB_FAIL(row_queue_.add_empty_row(obj_copy_allocator_))) {
        LOG_WARN("Failed to add empty row into row queue", K(ret));
      } else if (OB_FAIL(compact_border_row(false/*last_row*/))) {
        LOG_WARN("Failed to compact first row", K(ret));
      } else if (OB_FAIL(check_compact_finish(compact_finish))) {
        LOG_WARN("Failed to check compact finish", K(ret));
      }
      while (OB_SUCC(ret) && !compact_finish) {
        if (OB_FAIL(inner_next(true /*open_macro*/))) { // read to curr_row_
          LOG_WARN("Failed to inner next for compact first row", K(ret), K(*this), K(compact_finish));
        } else if (OB_ISNULL(curr_row_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected null current row", K(ret), K(*this));
        } else if (OB_FAIL(check_meet_another_trans())) { // will add empty row for different trans
          LOG_WARN("Fail to check meet another trans", K(ret), KPC_(curr_row), KPC(this));
        } else if (OB_FAIL(compact_border_row(false/*last_row*/))) {
          LOG_WARN("Failed to compact first row", K(ret));
        } else if (curr_row_->is_shadow_row()) {
          // continue
        } else if (OB_UNLIKELY(2 == row_queue_.count())) {
          // two trans row, row queue will have > 2 rows [shadow_row / trans_A row / empty row for trans_B]
          // one trans row, row queue will have 1 row [trans_A row]
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected row queue", K(ret), K(row_queue_.count()), KPC(this));
        } else if (row_queue_.count() > 1 && OB_FAIL(compact_border_row(true /*last_row */))) {
          // need to compact to last row
          LOG_WARN("Failed to compact current row to last row", K(ret));
        }

        if (FAILEDx(check_compact_finish(compact_finish))) {
          LOG_WARN("Failed to check compact finish", K(ret));
        } else if (curr_row_->is_last_multi_version_row()) {
          check_committing_trans_compacted_ = true;
        }
      } // end of while

      if (OB_SUCC(ret)) {
        LOG_DEBUG("make committing trans compacted", K(ret), KPC(curr_row_),
                    KPC(row_queue_.get_first()), KPC(row_queue_.get_last()), K(row_queue_.count()));
        if (!row_queue_.has_next()) { // get row from row_queue
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected empty row queue", K(ret), K_(row_queue));
        } else if (OB_FAIL(row_queue_.get_next_row(curr_row_))) {
          LOG_WARN("Failed to get next row from row_queue", K(ret));
        }
      }
    }
  } else if (curr_row_->is_shadow_row()) {
    // skip shadow row because compact finished before
    if (OB_FAIL(inner_next(true /*open_macro*/))) { // read to curr_row_
      LOG_WARN("Failed to get nex row", K(ret), KPC(this));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(curr_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpceted null current row", K(ret), KP_(curr_row));
  } else if (curr_row_->is_ghost_row()) {
    ++ghost_row_count_;
  }
  LOG_DEBUG("make commited trans row compacted", KPC(curr_row_));

  return ret;
}

int ObPartitionMinorRowMergeIter::next()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionRowMergeIter is not inited", K(ret), K(*this));
  } else if (OB_ISNULL(row_iter_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("Unexpected null row iter", K(ret), K(*this));
  } else if (OB_UNLIKELY(iter_end_)) {
    ret = OB_ITER_END;
  } else if (OB_LIKELY(curr_row_ != nullptr)) {
    is_rowkey_first_row_already_output_ = !curr_row_->is_last_multi_version_row();
    curr_row_ = nullptr;
  }

  if (OB_FAIL(ret)) {
  } else if (row_queue_.has_next()) { // get row from row_queue
    if (OB_FAIL(row_queue_.get_next_row(curr_row_))) {
      LOG_WARN("Failed to get next row from row_queue", K(ret));
    }
  } else if (OB_FAIL(inner_next(false /*open_macro*/))) {
    if (OB_UNLIKELY(ret != OB_ITER_END)) {
      LOG_WARN("Failed to inner next row", K(ret));
    }
  } else if (OB_FAIL(skip_ghost_row())) {
    if (OB_UNLIKELY(ret != OB_ITER_END)) {
      LOG_WARN("Failed to skip_ghost_row", K(ret));
    }
  } else if (OB_ISNULL(curr_row_)) {
    if (typeid(*this) == typeid(ObPartitionMinorRowMergeIter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpceted null current row", K(ret), K(*this));
    }
  } else if (need_recycle_mv_row()) {
    if (OB_FAIL(compact_old_row())) {
      LOG_WARN("Failed to compact_old_row", K(ret));
    }
  } else if (OB_FAIL(try_make_committing_trans_compacted())) {
    LOG_WARN("Failed to make committing trans compacted", K(ret), K(*this));
  }
  if (OB_SUCC(ret) && curr_row_ != nullptr && curr_row_->is_last_multi_version_row()) {
    check_committing_trans_compacted_ = true;
  }
  return ret;
}

int ObPartitionMinorRowMergeIter::skip_ghost_row()
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && curr_row_ != nullptr && curr_row_->is_ghost_row() && !is_rowkey_first_row_already_output()) {
    curr_row_ = nullptr;
    if (OB_FAIL(inner_next(false /*open_macro*/))) {
      if (OB_UNLIKELY(ret != OB_ITER_END)) {
        LOG_WARN("Failed to inner next row", K(ret));
      }
    }
  }
  return ret;
}
int ObPartitionMinorRowMergeIter::compare_multi_version_col(const ObPartitionMergeIter &other,
                                                            const int64_t multi_version_col,
                                                            int &cmp_ret)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(multi_version_col <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid multi version column index", K(ret), K(multi_version_col));
  } else if (OB_UNLIKELY(nullptr == curr_row_ || nullptr == other.get_curr_row())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected the cur row and other cur row should not be NULL",
                K(ret), KP_(curr_row), KP(other.get_curr_row()));
  } else if (OB_UNLIKELY(curr_row_->count_ <= multi_version_col
                         || other.get_curr_row()->count_ <= multi_version_col)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected column cnt to compare multi version col",
                K(ret), KPC(curr_row_), KPC(other.get_curr_row()));
  } else {
    const int64_t multi_value = curr_row_->storage_datums_[multi_version_col].get_int();
    const int64_t other_multi_value = other.get_curr_row()->storage_datums_[multi_version_col].get_int();
    if (multi_value < other_multi_value) {
      cmp_ret = -1;
    } else if (multi_value > other_multi_value) {
      cmp_ret = 1;
    } else {
      // during replay after reboot, there may be the same multi-version row between memtable and sstable
      cmp_ret = 0;
    }
    LOG_DEBUG("multi version compare two iters", K(cmp_ret), K(multi_value), K(other_multi_value));
  }

  return ret;
}

int ObPartitionMinorRowMergeIter::multi_version_compare(const ObPartitionMergeIter &other,
                                                        int &cmp_ret)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObPartitionMinorRowMergeIter has not been inited", K(ret));
  } else if (OB_UNLIKELY(!other.is_multi_version_minor_iter())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Unexpected compare iter type for multi version compare", K(ret), K(other));
  } else if (OB_FAIL(compare_multi_version_col(other, schema_rowkey_column_cnt_, cmp_ret))) {
    LOG_WARN("Failed to compare trans version column", K(ret));
  } else if (cmp_ret == 0 && curr_row_->is_uncommitted_row()
             && other.get_curr_row()->is_uncommitted_row()) { // compare sql_sequence
    if (OB_FAIL(compare_multi_version_col(other, schema_rowkey_column_cnt_ + 1, cmp_ret))) {
      LOG_WARN("Failed to compare sql sequence column", K(ret));
    }
  }

  return ret;
}

// committing row means that the row is stored as uncommitted_row, iter as commited row
bool ObPartitionMinorRowMergeIter::is_curr_row_commiting() const
{
  bool bret = false;
  if (OB_NOT_NULL(curr_row_)) {
    bret = !curr_row_->is_uncommitted_row()
      && !curr_row_->is_shadow_row()
      && curr_row_->storage_datums_[schema_rowkey_column_cnt_ + 1].get_int() < 0;
  }
  return bret;
}

int ObPartitionMinorRowMergeIter::collect_tnode_dml_stat(
    storage::ObTransNodeDMLStat &tnode_stat) const
{
  int ret = OB_SUCCESS;
  memtable::ObMemtableMultiVersionScanIterator *iter = nullptr;

  if (OB_UNLIKELY(nullptr == table_ || nullptr == row_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null table or null row_iter", KPC(table_), KPC(row_iter_));
  } else if (OB_UNLIKELY(!table_->is_data_memtable() ||
      typeid(*row_iter_) != typeid(memtable::ObMemtableMultiVersionScanIterator))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only support to get mt stat from tx memtable", K(ret), KPC(table_), KPC(row_iter_));
  } else if (OB_ISNULL(iter = static_cast<memtable::ObMemtableMultiVersionScanIterator *>(row_iter_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null row iter", K(ret), KPC(row_iter_));
  } else if (OB_FAIL(iter->get_tnode_stat(tnode_stat))) {
    LOG_WARN("failed to get mt stat", K(ret));
  }
  return ret;
}


/*
 *ObPartitionMinorMacroMergeIter
 */
ObPartitionMinorMacroMergeIter::ObPartitionMinorMacroMergeIter(common::ObIAllocator &allocator, bool reuse_uncommit_row)
  : ObPartitionMinorRowMergeIter(allocator),
    macro_block_iter_(nullptr),
    curr_block_desc_(),
    curr_block_meta_(),
    macro_block_opened_(false),
    last_macro_block_reused_(-1),
    last_macro_block_recycled_(false),
    last_mvcc_row_already_output_(true),
    have_macro_output_row_(false),
    reuse_uncommit_row_(reuse_uncommit_row)
{
  curr_block_desc_.macro_meta_ = &curr_block_meta_;
}

ObPartitionMinorMacroMergeIter::~ObPartitionMinorMacroMergeIter()
{
  reset();
}

void ObPartitionMinorMacroMergeIter::reset()
{
  if (nullptr != macro_block_iter_) {
    macro_block_iter_->~ObIMacroBlockIterator();
    allocator_.free(macro_block_iter_);
    macro_block_iter_ = nullptr;
  }
  curr_block_desc_.reset();
  curr_block_meta_.reset();
  curr_block_desc_.macro_meta_ = &curr_block_meta_;
  macro_block_opened_ = false;
  last_macro_block_reused_ = -1;
  last_macro_block_recycled_ = false;
  last_mvcc_row_already_output_ = true;
  have_macro_output_row_ = false;
  ObPartitionMinorRowMergeIter::reset();
}

bool ObPartitionMinorMacroMergeIter::inner_check(const ObMergeParameter &merge_param)
{
  bool bret = true;

  if (!table_->is_sstable()) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "Unexpected table type for minor row merge iter", K(bret), KPC(table_));
  } else {
    bret = ObPartitionMinorRowMergeIter::inner_check(merge_param);
  }

  return bret;
}

int ObPartitionMinorMacroMergeIter::inner_init(const ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  const ObITableReadInfo *rowkey_read_info_ = merge_param.static_param_.rowkey_read_info_;
  if (OB_ISNULL(rowkey_read_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null index read info", K(ret));
  } else if (OB_FAIL(common_minor_inner_init(merge_param))) {
    LOG_WARN("Failed to do commont minor inner init", K(ret), K(merge_param));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObSSTableRowWholeScanner)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc memory for minor merge row scanner", K(ret));
  } else {
    row_iter_ = new (buf) ObSSTableRowWholeScanner();
    macro_block_opened_ = false;
    last_macro_block_reused_ = -1;
    last_macro_block_recycled_ = false;
    last_mvcc_row_already_output_ = true;
    have_macro_output_row_ = false;
    ObSSTable *sstable = static_cast<ObSSTable *>(table_);
    if (OB_FAIL(sstable->scan_macro_block(
        merge_range_,
        *rowkey_read_info_,
        allocator_,
        macro_block_iter_,
        false, /* reverse scan */
        false, /* need micro info */
        true /* need secondary meta */))) {
      LOG_WARN("Fail to scan macro block", K(ret));
    }
  }

  return ret;
}

int ObPartitionMinorMacroMergeIter::check_need_open_curr_macro_block(bool &need)
{
  int ret = OB_SUCCESS;
  need = false;
  if (!reuse_uncommit_row_ && curr_block_desc_.contain_uncommitted_row_) {
    need = true;
    LOG_INFO("need rewrite one dirty macro", K_(curr_block_desc));
  //TODO:only for recyle multi version row
  // } else if ((last_macro_block_recycled_ && !last_mvcc_row_already_output_) ||
  //            (!curr_block_desc_.contain_uncommitted_row_ &&
  //             curr_block_desc_.max_merged_trans_version_ <= access_context_.trans_version_range_.base_version_)) {
    // 1. last_macro_recycled and current_macro can not be recycled:
    //    need to open to recycle left rows of the last rowkey in recycled macro block
    // 2. last_macro_reused and current can be recycled: need to open to recycle micro blocks
    need = true;
  } else if (OB_FAIL(check_merge_range_cross(curr_block_desc_.range_, need))) {
    LOG_WARN("failed to check range cross", K(ret), K(curr_block_desc_.range_));
  }
  LOG_DEBUG("check macro block need open", K(curr_block_desc_.range_), K(merge_range_), K(need));
  return ret;
}

int ObPartitionMinorMacroMergeIter::check_macro_block_recycle(const ObMacroBlockDesc &macro_desc, bool &can_recycle)
{
  int ret = OB_SUCCESS;
  can_recycle = false;
  if (OB_UNLIKELY(!macro_desc.is_valid() || !access_context_.query_flag_.is_multi_version_minor_merge())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid block data", K(ret), K(macro_desc), K(access_context_));
  } else if (!macro_desc.contain_uncommitted_row_ &&
             macro_desc.max_merged_trans_version_ <= access_context_.trans_version_range_.base_version_ &&
             (last_macro_block_recycled_ || last_mvcc_row_already_output_)) {
    can_recycle = true;
  }
  // TODO: @dengzhi.ldz enable recycle after making adaptor for migration
  can_recycle = false;
  return ret;
}

// next_range and open_curr_macro_block are the same with major macro merge iter
int ObPartitionMinorMacroMergeIter::next_range()
{
  int ret = OB_SUCCESS;
  bool can_recycle = false;
  if (OB_UNLIKELY(iter_end_)) {
    ret = OB_ITER_END;
  } else {
    while (OB_SUCC(ret) && !iter_end_) {
      if (-1 == last_macro_block_reused_) {
        last_mvcc_row_already_output_ = true;
      } else if (OB_FAIL(!curr_block_desc_.is_valid_with_macro_meta())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid macro meta", K(ret), K(curr_block_desc_));
      } else {
        last_mvcc_row_already_output_ = curr_block_desc_.macro_meta_->is_last_row_last_flag();
      }
      if (OB_FAIL(ret)) {
      } else if (FALSE_IT(reset_macro_block_desc())) {
      } else if (OB_SUCC(macro_block_iter_->get_next_macro_block(curr_block_desc_))) {
        if (-1 == last_macro_block_reused_) {
          last_macro_block_reused_ = 0;
        } else {
          last_macro_block_reused_ = !macro_block_opened_;
          if (!macro_block_opened_) {
            last_macro_block_recycled_  = false;
          }
        }
        macro_block_opened_ = false;
        have_macro_output_row_ = false;
        is_rowkey_shadow_row_reused_ = false;
        if (OB_FAIL(check_macro_block_recycle(curr_block_desc_, can_recycle))) {
          LOG_WARN("failed to check macro block recycle", K(ret));
        } else if (can_recycle) {
          macro_block_opened_ = true;
          last_macro_block_recycled_ = true;
          FLOG_INFO("macro block recycled", K(curr_block_desc_.macro_block_id_));
        } else {
          break;
        }
      } else if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Failed to get next macro block", K(ret));
      } else {
        iter_end_ = true;
      }
    }
  }

  return ret;
}

int ObPartitionMinorMacroMergeIter::open_curr_macro_block()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(macro_block_opened_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("Unepxcted opened macro block to open", K(ret));
  } else {
    bool is_first_row = false;
    bool is_shadow_row = false;
    ObSSTableRowWholeScanner *iter = reinterpret_cast<ObSSTableRowWholeScanner *>(row_iter_);
    iter->reuse();
    if (OB_FAIL(iter->open(
                access_param_.iter_param_,
                access_context_,
                merge_range_,
                curr_block_desc_,
                *reinterpret_cast<ObSSTable *>(table_),
                last_mvcc_row_already_output_))) {
      LOG_WARN("fail to set context", K(ret));
    } else if (OB_FAIL(iter->get_first_row_mvcc_info(is_first_row, is_shadow_row))) {
        LOG_WARN("Fail to check rowkey first row info", K(ret), KPC(iter));
    } else {
      macro_block_opened_ = true;
      if (!row_queue_.has_next()) {
        is_rowkey_first_row_already_output_ = !is_first_row;
      }
      if (last_macro_block_reused() && last_macro_block_recycled_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected merge status", K(ret), K(curr_block_desc_.macro_block_id_), KPC(this));
      } else if (last_macro_block_reused()) {
        check_committing_trans_compacted_ = is_first_row;
        is_rowkey_shadow_row_reused_ = !is_first_row && !is_shadow_row;
      } else if (last_macro_block_recycled_) {
        last_macro_block_recycled_ = false;
        check_committing_trans_compacted_ = true;
        is_rowkey_first_row_already_output_ = false;
        is_rowkey_shadow_row_reused_ = false;
        if (OB_FAIL(recycle_last_rowkey_in_macro_block(*iter))) {
          LOG_WARN("Fail to recycle last rowkey in current macro block", K(ret), K(curr_block_desc_.macro_block_id_));
        }
      }
    }
  }

  return ret;
}

int ObPartitionMinorMacroMergeIter::recycle_last_rowkey_in_macro_block(ObSSTableRowWholeScanner &iter)
{
  int ret = OB_SUCCESS;
  bool is_rowkey_first_row = false;
  bool is_rowkey_first_shadow_row = false;
  if (OB_UNLIKELY(!access_context_.query_flag_.is_multi_version_minor_merge())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid block data", K(ret), K_(access_context));
  } else if (OB_FAIL(iter.get_first_row_mvcc_info(is_rowkey_first_row, is_rowkey_first_shadow_row))) {
    LOG_WARN("Fail to get first row info", K(ret));
  } else if (!is_rowkey_first_row) {
    // recycle left rows of the last rowkey in current micro block
    const blocksstable::ObDatumRow *row = nullptr;
    int64_t trans_col_index = schema_rowkey_column_cnt_;
    const int64_t recycle_version = access_context_.trans_version_range_.base_version_;
    while(OB_SUCC(ret)) {
      if (OB_FAIL(iter.get_next_row(row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Failed to get next row", K(ret), K(iter));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpceted meet end of the iter", K(ret), K(iter));
        }
      } else if (OB_UNLIKELY(row->is_uncommitted_row() ||
                             -row->storage_datums_[trans_col_index].get_int() > recycle_version)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected trans version in row", K(ret), K(recycle_version), KPC(row),
                 K(curr_block_desc_.macro_block_id_));
      } else if (row->is_last_multi_version_row()) {
        break;
      }
    }
  }
  return ret;
}

int ObPartitionMinorMacroMergeIter::next()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPartitionMinorRowMergeIter::next())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("Failed to get minor macro next", K(ret));
    }
  } else {
    if (have_macro_output_row_) {
      is_rowkey_first_row_already_output_ = false;
      is_rowkey_shadow_row_reused_ = false;
    } else if (nullptr != curr_row_) {
      have_macro_output_row_ = true;
    }
  }
  return ret;
}

int ObPartitionMinorMacroMergeIter::inner_next(const bool open_macro)
{
  int ret = OB_SUCCESS;
  bool need_check = false;
  if (macro_block_opened_ && OB_SUCC(row_iter_->get_next_row(curr_row_))) {
    iter_row_count_++;
  } else if (OB_UNLIKELY(OB_SUCCESS != ret && OB_ITER_END != ret)) {
    LOG_WARN("Failed to get next row", K(ret), K(*this));
  } else {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      if (macro_block_opened_) {
        if (!curr_block_meta_.val_.is_last_row_last_flag_ && !is_rowkey_first_row_already_output()) {
          need_check = true;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(next_range())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Failed to get next range", K(ret), K(*this));
      }
    } else if (!open_macro && !need_check && OB_FAIL(check_need_open_curr_macro_block(need_check))) {
      STORAGE_LOG(WARN, "Failed to check need open curr macro block", K(ret));
    } else if (open_macro || need_check) {
      if (OB_FAIL(open_curr_macro_block())) {
        LOG_WARN("Failed to open current macro block", K(ret), K(open_macro));
      } else if (OB_FAIL(inner_next(open_macro))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("Failed to inner next row", K(ret),KPC(this));
        }
      } else {
        LOG_DEBUG("open macro block on demand", K(open_macro), K(need_check), KPC(this));
      }
    }
  }

  return ret;
}

int ObPartitionMinorMacroMergeIter::open_curr_range(const bool for_rewrite, const bool for_compare)
{
  int ret = OB_SUCCESS;
  UNUSED(for_rewrite);
  const ObLogicMacroBlockId curr_macro_logic_id_ = curr_block_desc_.macro_meta_->get_logic_id();

  if (OB_FAIL(open_curr_macro_block())) {
    LOG_WARN("Failed to open curr macro block", K(ret));
  } else if (OB_FAIL(next())) {
    if (for_compare && ret == OB_ITER_END) {
      ret = OB_BLOCK_SWITCHED;
      LOG_INFO("curr macro block changed", K(curr_block_desc_));
    } else if (ret != OB_ITER_END) {
      STORAGE_LOG(WARN, "failed to next", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (for_compare && curr_macro_logic_id_ != curr_block_desc_.macro_meta_->get_logic_id()) {
    LOG_INFO("curr macro block changed", K(curr_block_desc_));
    ret = OB_BLOCK_SWITCHED;
  }

  return ret;
}

int ObPartitionMinorMacroMergeIter::get_curr_range(ObDatumRange &range) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionMinorMacroMergeIter is not inited", K(ret), K(*this));
  } else if (macro_block_opened_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected opened macro block to get range", K(ret), K(*this));
  } else {
    range = curr_block_desc_.range_;
    revise_macro_range(range);
    range.set_left_closed();
    range.set_right_closed();
  }
  return ret;
}

} //compaction
} //oceanbase
