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

#include "common/sql_mode/ob_sql_mode_utils.h"
#include "ob_sstable_row_iterator.h"
#include "share/config/ob_server_config.h"
#include "lib/stat/ob_diagnose_info.h"
#include "blocksstable/ob_lob_data_reader.h"
#include "storage/ob_file_system_util.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

namespace oceanbase {
namespace storage {
/**
 * ---------------------------------------------------ObSSTableReadHandle------------------------------------------------
 */
ObSSTableReadHandle::ObSSTableReadHandle()
    : flag_(0),
      state_(0),
      is_bf_contain_(false),
      range_idx_(0),
      macro_block_ctx_(),
      full_meta_(),
      macro_idx_(0),
      index_handle_(),
      micro_begin_idx_(0),
      micro_end_idx_(0),
      ext_rowkey_(NULL),
      row_handle_(),
      rowkey_helper_(NULL)
{}

ObSSTableReadHandle::~ObSSTableReadHandle()
{}

void ObSSTableReadHandle::reset()
{
  flag_ = 0;
  state_ = 0;
  is_bf_contain_ = false;
  range_idx_ = 0;
  macro_block_ctx_.reset();
  full_meta_.reset();
  macro_idx_ = 0;
  index_handle_.reset();
  micro_begin_idx_ = 0;
  micro_end_idx_ = 0;
  ext_rowkey_ = NULL;
  row_handle_.reset();
  rowkey_helper_ = NULL;
}

int ObSSTableReadHandle::get_index_handle(ObMicroBlockIndexHandle*& index_handle)
{
  int ret = OB_SUCCESS;
  if (!macro_block_ctx_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get_index_handle should be called after a valid macro_block_ctx_", K(ret));
  } else {
    index_handle = &index_handle_;
  }
  return ret;
}

/**
 * ------------------------------------------------ObSSTableMicroBlockInfoIterator--------------------------------------------
 */
ObSSTableMicroBlockInfoIterator::ObSSTableMicroBlockInfoIterator()
    : is_reverse_(false),
      total_micro_cnt_(0),
      cur_micro_idx_(0),
      step_(0),
      macro_block_ctx_(),
      micro_block_infos_(),
      iter_(NULL),
      handle_(NULL),
      is_get_(false)
{}

ObSSTableMicroBlockInfoIterator::~ObSSTableMicroBlockInfoIterator()
{}

void ObSSTableMicroBlockInfoIterator::reset()
{
  is_reverse_ = false;
  total_micro_cnt_ = 0;
  cur_micro_idx_ = 0;
  step_ = 0;
  micro_block_infos_.reset();
  iter_ = NULL;
  handle_ = NULL;
  is_get_ = false;
  micro_info_.reset();
}

void ObSSTableMicroBlockInfoIterator::reuse()
{
  is_reverse_ = false;
  total_micro_cnt_ = 0;
  cur_micro_idx_ = 0;
  step_ = 0;
  micro_block_infos_.reuse();
  iter_ = NULL;
  handle_ = NULL;
  is_get_ = false;
  micro_info_.reset();
}

int ObSSTableMicroBlockInfoIterator::open(ObSSTableRowIterator* iter, ObSSTableReadHandle& read_handle)
{
  int ret = OB_SUCCESS;
  ObSSTableSkipRangeCtx* skip_ctx = NULL;
  cur_micro_idx_ = -1;
  if (OB_ISNULL(iter)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(iter));
  } else if (OB_FAIL(iter->get_skip_range_ctx(read_handle, INT64_MAX, skip_ctx))) {
    STORAGE_LOG(WARN, "fail to get skip range ctx", K(ret));
  } else if (NULL != skip_ctx && skip_ctx->need_skip_) {
    // need skip current macro block
    ret = OB_BEYOND_THE_RANGE;
  } else if (0 != read_handle.is_get_) {
    is_get_ = true;
    // get
    if (ObSSTableRowState::IN_BLOCK == read_handle.state_) {
      ObMicroBlockIndexHandle* handle = NULL;
      if (OB_FAIL(read_handle.get_index_handle(handle))) {
        STORAGE_LOG(WARN, "Fail to get index handle, ", K(ret), K(read_handle));
      } else if (OB_FAIL(handle->search_blocks(
                     read_handle.ext_rowkey_->get_store_rowkey(), micro_info_, iter->get_rowkey_cmp_funcs()))) {
        if (OB_BEYOND_THE_RANGE != ret) {
          STORAGE_LOG(WARN, "Fail to search blocks, ", K(ret), K(read_handle));
        }
      }
    }
  } else {
    is_get_ = false;
    // scan
    ObMicroBlockIndexHandle* handle = NULL;
    if (OB_FAIL(read_handle.get_index_handle(handle))) {
      STORAGE_LOG(WARN, "Fail to get index handle, ", K(ret), K(read_handle));
    } else if (OB_FAIL(handle->search_blocks(read_handle.ext_range_->get_range(),
                   read_handle.is_left_border_,
                   read_handle.is_right_border_,
                   micro_block_infos_,
                   iter->get_rowkey_cmp_funcs()))) {
      if (OB_BEYOND_THE_RANGE != ret) {
        STORAGE_LOG(WARN, "Fail to search blocks, ", K(ret), K(read_handle));
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_BEYOND_THE_RANGE == ret) {
      ret = OB_SUCCESS;
      read_handle.state_ = ObSSTableRowState::NOT_EXIST;
    }
  } else if (ObSSTableRowState::IN_BLOCK == read_handle.state_) {
    macro_block_ctx_ = read_handle.macro_block_ctx_;
    read_handle.micro_begin_idx_ = total_micro_cnt_;
    read_handle.micro_end_idx_ = total_micro_cnt_ + get_micro_block_info_count() - 1;
    cur_micro_idx_ = is_reverse_ ? get_micro_block_info_count() - 1 : 0;
    iter_ = iter;
    handle_ = &read_handle;
    STORAGE_LOG(
        DEBUG, "prepare macro block prefetch", K(read_handle), K(get_micro_block_info_count()), K(micro_block_infos_));
  }

  return ret;
}

int ObSSTableMicroBlockInfoIterator::get_next_micro(ObSSTableMicroBlockInfo& sstable_micro)
{
  int ret = OB_SUCCESS;
  ObSSTableSkipRangeCtx* skip_ctx = NULL;
  if (cur_micro_idx_ < 0 || cur_micro_idx_ >= get_micro_block_info_count()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(iter_->get_skip_range_ctx(*handle_, total_micro_cnt_, skip_ctx))) {
    STORAGE_LOG(WARN, "fail to get skip range ctx", K(ret));
  } else {
    sstable_micro.macro_ctx_ = macro_block_ctx_;
    sstable_micro.micro_info_ = get_micro_block_info(cur_micro_idx_);
    sstable_micro.micro_idx_ = total_micro_cnt_++;
    sstable_micro.is_skip_ = NULL != skip_ctx && skip_ctx->need_skip_;
    cur_micro_idx_ = cur_micro_idx_ + step_;
  }
  return ret;
}

/**
 * ---------------------------------------------------ObISSTableRowIterator--------------------------------------------------
 */

void ObISSTableRowIterator::reset()
{
  if (OB_NOT_NULL(lob_reader_)) {
    lob_reader_->~ObLobDataReader();
    lob_reader_ = NULL;
    if (OB_NOT_NULL(lob_allocator_)) {
      lob_allocator_->free(lob_reader_);
    } else {
      STORAGE_LOG(WARN, "[LOB] unexpected lob_reader with null allocator");
    }
  }
  lob_allocator_ = NULL;
  sstable_ = NULL;
  batch_rows_ = NULL;
  batch_row_count_ = 0;
  batch_row_pos_ = 0;
}

void ObISSTableRowIterator::reuse()
{
  if (OB_NOT_NULL(lob_reader_)) {
    lob_reader_->reuse();
  }

  batch_rows_ = NULL;
  batch_row_count_ = 0;
  batch_row_pos_ = 0;
}

int ObISSTableRowIterator::init(
    const ObTableIterParam& iter_param, ObTableAccessContext& access_ctx, ObITable* table, const void* query_range)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(query_range) || OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init ObISSTableRowIterator", K(ret), KP(query_range), KP(table));
  } else if (OB_FAIL(inner_open(iter_param, access_ctx, table, query_range))) {
    STORAGE_LOG(WARN, "Failed to inner open ObISSTableRowIterator", K(ret));
  } else {
    sstable_ = static_cast<ObSSTable*>(table);
    if (sstable_->has_lob_macro_blocks()) {
      bool has_lob_column = false;
      if (OB_FAIL(iter_param.has_lob_column_out(access_ctx.use_fuse_row_cache_, has_lob_column))) {
        STORAGE_LOG(WARN, "fail to check has lob column", K(ret));
      } else if (has_lob_column) {
        if (OB_FAIL(add_lob_reader(iter_param, access_ctx, *sstable_))) {
          STORAGE_LOG(WARN, "Failed to add lob reader", K(ret));
        } else if (OB_ISNULL(lob_reader_)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected null pointer after add lob reader", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObISSTableRowIterator::get_next_row(const ObStoreRow*& store_row)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(inner_get_next_row(store_row))) {
  } else {
    if (has_lob_column() && ObActionFlag::OP_ROW_DOES_NOT_EXIST != store_row->flag_) {
      if (OB_FAIL(read_lob_columns(store_row))) {
        STORAGE_LOG(WARN, "Failed to read lob columns from store row", K(ret));
      }
    }
  }

  return ret;
}

int ObISSTableRowIterator::add_lob_reader(
    const ObTableIterParam& iter_param, ObTableAccessContext& access_ctx, ObSSTable& sstable)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;

  if (OB_NOT_NULL(lob_reader_)) {
    // to optimize rescan ,some iters will init after only reuse, the lob reader is not reseted
    lob_reader_->~ObLobDataReader();
    lob_reader_ = NULL;
    lob_allocator_ = NULL;
  }
  if (OB_ISNULL(lob_allocator_ = access_ctx.allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Unexpected null allocator to add lob reader", K(ret));
  } else if (OB_ISNULL(ptr = lob_allocator_->alloc(sizeof(blocksstable::ObLobDataReader)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Failed to allocate memory for lob reader", K(ret));
  } else {
    lob_reader_ = new (ptr) blocksstable::ObLobDataReader();
    if (OB_SUCC(lob_reader_->init(access_ctx.query_flag_.is_daily_merge(), sstable))) {
      STORAGE_LOG(DEBUG,
          "[LOB] success to add lob reader for ObISSTableRowIterator",
          K(sstable),
          K(access_ctx.query_flag_),
          K(iter_param));
    } else {
      STORAGE_LOG(WARN,
          "Failed to init lob reader for ObISSTableRowIterator",
          K(sstable),
          K(access_ctx.query_flag_),
          K(iter_param),
          K(ret));
      lob_reader_->~ObLobDataReader();
      lob_allocator_->free(lob_reader_);
      lob_reader_ = NULL;
      lob_allocator_ = NULL;
    }
  }

  return ret;
}

int ObISSTableRowIterator::read_lob_columns(const ObStoreRow* store_row)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(lob_reader_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLobDataReader not init for ObISSTableRowIterator", K(ret));
  } else if (OB_ISNULL(store_row)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid row to read lob columns", K(ret));
  } else {
    ObStoreRow* row = const_cast<ObStoreRow*>(store_row);
    lob_reader_->reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < row->row_val_.count_; ++i) {
      ObObj& obj = row->row_val_.cells_[i];
      if (ob_is_text_tc(obj.get_type())) {
        if (obj.is_lob_outrow()) {
          if (OB_FAIL(lob_reader_->read_lob_data(obj, obj))) {
            STORAGE_LOG(WARN, "Failed to read lob obj", K(obj.get_scale()), K(obj.get_meta()), K(obj.val_len_), K(ret));
          } else {
            STORAGE_LOG(
                DEBUG, "[LOB] Succeed to load lob obj", K(obj.get_scale()), K(obj.get_meta()), K(obj.val_len_), K(ret));
          }
        } else if (obj.is_lob_inrow()) {
        } else {
          STORAGE_LOG(ERROR,
              "[LOB] Unexpected lob obj scale, to compatible, change to inrow mode",
              K(obj),
              K(obj.get_scale()),
              K(ret));
          obj.set_lob_inrow();
        }
      }
    }
  }

  return ret;
}

int ObISSTableRowIterator::batch_get_next_row(ObIMicroBlockRowScanner* scanner, const ObStoreRow*& store_row)
{
  int ret = OB_SUCCESS;
  store_row = NULL;
  if (OB_ISNULL(scanner)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(scanner));
  } else if (OB_NOT_NULL(batch_rows_) && batch_row_pos_ < batch_row_count_) {
    store_row = batch_rows_ + batch_row_pos_++;
  } else if (OB_FAIL(scanner->get_next_rows(batch_rows_, batch_row_count_))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to get next rows", K(ret));
    }
  } else if (OB_ISNULL(batch_rows_) || batch_row_count_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid batch rows", K(ret), KP(batch_rows_), K(batch_row_count_));
  } else {
    store_row = batch_rows_;
    batch_row_pos_ = 1;
  }
  if (OB_FAIL(ret)) {
    batch_rows_ = NULL;
    batch_row_count_ = 0;
    batch_row_pos_ = 0;
  }
  return ret;
}

/**
 * ---------------------------------------------------ObSSTableRowIterator--------------------------------------------------
 */
ObSSTableRowIterator::ObSSTableRowIterator()
    : iter_param_(NULL),
      access_ctx_(NULL),
      sstable_(NULL),
      query_range_(NULL),
      scan_step_(0),
      read_handle_cnt_(0),
      micro_handle_cnt_(0),
      macro_block_iter_(),
      block_index_handle_mgr_(),
      block_handle_mgr_(),
      table_store_stat_(),
      skip_ctx_(),
      storage_file_(nullptr),
      is_opened_(false),
      is_base_(false),
      block_cache_(NULL),
      table_type_(ObITable::MAJOR_SSTABLE),
      sstable_snapshot_version_(0),
      block_reader_(),
      micro_exister_(NULL),
      micro_getter_(NULL),
      micro_lock_checker_(NULL),
      micro_scanner_(NULL),
      read_handles_(),
      prefetch_handle_end_(false),
      prefetch_block_end_(false),
      cur_prefetch_handle_pos_(0),
      cur_fetch_handle_pos_(0),
      cur_read_handle_pos_(0),
      cur_prefetch_micro_pos_(0),
      cur_read_micro_pos_(0),
      cur_micro_idx_(-1),
      cur_range_idx_(-1),
      sstable_micro_infos_(),
      micro_handles_(),
      io_micro_infos_(),
      micro_info_iter_(),
      prefetch_handle_depth_(DEFAULT_PREFETCH_HANDLE_DEPTH),
      prefetch_micro_depth_(DEFAULT_PREFETCH_MICRO_DEPTH)
{}

ObSSTableRowIterator::~ObSSTableRowIterator()
{
  if (NULL != micro_exister_) {
    micro_exister_->~ObMicroBlockRowExister();
    micro_exister_ = NULL;
  }
  if (NULL != micro_getter_) {
    micro_getter_->~ObMicroBlockRowGetter();
    micro_getter_ = NULL;
  }
  if (NULL != micro_scanner_) {
    micro_scanner_->~ObIMicroBlockRowScanner();
    micro_scanner_ = NULL;
  }
  if (NULL != micro_lock_checker_) {
    micro_lock_checker_->~ObMicroBlockRowLockChecker();
    micro_lock_checker_ = NULL;
  }
}

int ObSSTableRowIterator::inner_open(
    const ObTableIterParam& iter_param, ObTableAccessContext& access_ctx, ObITable* table, const void* query_range)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The ObSSTableRowIterator has been opened, ", K(ret));
  } else if (OB_UNLIKELY(NULL == query_range || NULL == table)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), KP(query_range), KP(table));
  } else if (OB_FAIL(get_handle_cnt(query_range, read_handle_cnt_, micro_handle_cnt_))) {
    STORAGE_LOG(WARN, "Fail to get handle cnt, ", K(ret), KP(query_range));
  } else if (OB_UNLIKELY(read_handle_cnt_ <= 0) || OB_UNLIKELY(micro_handle_cnt_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected error, ", K(ret), K_(read_handle_cnt), K_(micro_handle_cnt));
  } else if (OB_FAIL(init_handle_mgr(iter_param, access_ctx, query_range))) {
    STORAGE_LOG(WARN, "fail to init handle mgr", K(ret), K(iter_param), K(access_ctx));
  } else if (OB_FAIL(read_handles_.reserve(*access_ctx.allocator_, read_handle_cnt_))) {
    STORAGE_LOG(WARN, "failed to reserve read handles", K(ret), K_(read_handle_cnt));
  } else if (OB_FAIL(micro_handles_.reserve(*access_ctx.allocator_, micro_handle_cnt_))) {
    STORAGE_LOG(WARN, "failed to reserve micro handles", K(ret), K_(micro_handle_cnt));
  } else if (OB_FAIL(sstable_micro_infos_.reserve(*access_ctx.allocator_, micro_handle_cnt_))) {
    STORAGE_LOG(WARN, "failed to reserve sstable micro infos", K(ret), K_(micro_handle_cnt));
  } else if (OB_FAIL(sorted_sstable_micro_infos_.reserve(*access_ctx.allocator_, micro_handle_cnt_))) {
    STORAGE_LOG(WARN, "failed to reserve sorted sstable micro infos", K(ret), K_(micro_handle_cnt));
  } else {
    sstable_ = static_cast<ObSSTable*>(table);
    is_base_ = sstable_->is_major_sstable();
    iter_param_ = &iter_param;
    access_ctx_ = &access_ctx;
    query_range_ = query_range;
    sstable_snapshot_version_ = is_base_ ? 0 : sstable_->get_snapshot_version();
    table_type_ = sstable_->get_key().table_type_;
    scan_step_ = access_ctx_->query_flag_.is_reverse_scan() ? -1 : 1;
    micro_info_iter_.set_reverse(access_ctx_->query_flag_.is_reverse_scan());
    table_store_stat_.pkey_ = access_ctx_->pkey_;
    block_cache_ = &(ObStorageCacheSuite::get_instance().get_block_cache());
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(storage_file_ = sstable_->get_storage_file_handle().get_storage_file())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to get file handle", K(ret), K(sstable_->get_storage_file_handle()));
    } else if (OB_FAIL(prefetch())) {
      STORAGE_LOG(WARN, "Fail to prefetch data, ", K(ret));
    } else {
      is_opened_ = true;
    }
  }

  if (OB_UNLIKELY(!is_opened_)) {
    reset();
  }
  return ret;
}

int ObSSTableRowIterator::inner_get_next_row(const ObStoreRow*& store_row)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObSSTableRowIterator has not been opened, ", K(ret), KP(this));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(prefetch())) {
        STORAGE_LOG(WARN,
            "Fail to do prefetch, ",
            K(ret),
            K_(cur_read_handle_pos),
            K_(cur_prefetch_handle_pos),
            K_(read_handle_cnt),
            K_(prefetch_handle_end),
            K_(prefetch_block_end));
      } else if (cur_read_handle_pos_ >= cur_prefetch_handle_pos_) {
        if (OB_UNLIKELY(prefetch_handle_end_)) {
          ret = OB_ITER_END;
        } else {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN,
              "Cur read handle pos exceed cur prefetch handle pos, ",
              K(ret),
              K_(cur_read_handle_pos),
              K_(cur_prefetch_handle_pos),
              K_(prefetch_handle_end),
              K_(prefetch_block_end));
        }
      } else if (OB_UNLIKELY(cur_read_handle_pos_ >= cur_fetch_handle_pos_)) {
        // continue
        cur_read_micro_pos_ = cur_prefetch_micro_pos_;
        cur_micro_idx_ = -1;
      } else if (OB_FAIL(fetch_row(read_handles_[cur_read_handle_pos_ % read_handle_cnt_], store_row))) {
        if (OB_ITER_END == ret) {
          ++cur_read_handle_pos_;
          ret = OB_SUCCESS;
        } else if (OB_EAGAIN == ret) {
          ret = OB_SUCCESS;
        } else {
          STORAGE_LOG(WARN,
              "Fail to fetch row, ",
              K(ret),
              K_(cur_read_handle_pos),
              K_(cur_prefetch_handle_pos),
              K_(prefetch_handle_end),
              K_(prefetch_block_end));
        }
      } else {
        break;
      }
    }

    if (OB_SUCC(ret) && NULL != store_row) {
      const_cast<ObStoreRow*>(store_row)->from_base_ = is_base_;
      set_row_snapshot(*const_cast<ObStoreRow*>(store_row));

      if (iter_param_->need_scn_ && OB_FAIL(set_row_scn(store_row))) {
        STORAGE_LOG(WARN, "failed to set row scn", K(ret));
      }

      EVENT_INC(ObStatEventIds::SSSTORE_READ_ROW_COUNT);
      STORAGE_LOG(DEBUG, "inner get next row", K(*store_row));
    }
  }

  return ret;
}

void ObSSTableRowIterator::set_row_snapshot(ObStoreRow& row)
{
  if (row.snapshot_version_ != INT64_MAX) {
    row.snapshot_version_ =
        std::min(access_ctx_->trans_version_range_.snapshot_version_, sstable_->get_upper_trans_version());
  }
}

void ObSSTableRowIterator::reset()
{
  ObISSTableRowIterator::reset();
  read_handles_.reset();
  micro_handles_.reset();
  sstable_micro_infos_.reset();

  if (NULL != micro_exister_) {
    micro_exister_->~ObMicroBlockRowExister();
    micro_exister_ = NULL;
  }
  if (NULL != micro_getter_) {
    micro_getter_->~ObMicroBlockRowGetter();
    micro_getter_ = NULL;
  }
  if (NULL != micro_scanner_) {
    micro_scanner_->~ObIMicroBlockRowScanner();
    micro_scanner_ = NULL;
  }
  if (NULL != micro_lock_checker_) {
    micro_lock_checker_->~ObMicroBlockRowLockChecker();
    micro_lock_checker_ = NULL;
  }

  macro_block_iter_.reset();
  iter_param_ = NULL;
  access_ctx_ = NULL;
  sstable_ = NULL;
  query_range_ = NULL;
  scan_step_ = 0;
  is_opened_ = false;
  is_base_ = false;
  block_cache_ = NULL;
  table_type_ = ObITable::MAJOR_SSTABLE;
  sstable_snapshot_version_ = 0;
  prefetch_handle_end_ = false;
  prefetch_block_end_ = false;
  cur_prefetch_handle_pos_ = 0;
  cur_fetch_handle_pos_ = 0;
  cur_read_handle_pos_ = 0;
  cur_prefetch_micro_pos_ = 0;
  cur_read_micro_pos_ = 0;
  cur_micro_idx_ = -1;
  cur_range_idx_ = -1;
  io_micro_infos_.reset();
  micro_info_iter_.reset();
  block_index_handle_mgr_.reset();
  block_handle_mgr_.reset();
  table_store_stat_.reset();
  skip_ctx_.reset();
  storage_file_ = nullptr;
  prefetch_handle_depth_ = DEFAULT_PREFETCH_HANDLE_DEPTH;
  prefetch_micro_depth_ = DEFAULT_PREFETCH_MICRO_DEPTH;
}

void ObSSTableRowIterator::reuse()
{
  ObISSTableRowIterator::reuse();
  if (NULL != micro_scanner_) {
    micro_scanner_->rescan();
  }

  macro_block_iter_.reset();
  query_range_ = NULL;
  scan_step_ = 0;
  is_opened_ = false;
  block_cache_ = NULL;
  sstable_snapshot_version_ = 0;
  prefetch_handle_end_ = false;
  prefetch_block_end_ = false;
  cur_prefetch_handle_pos_ = 0;
  cur_fetch_handle_pos_ = 0;
  cur_read_handle_pos_ = 0;
  cur_prefetch_micro_pos_ = 0;
  cur_read_micro_pos_ = 0;
  cur_micro_idx_ = -1;
  cur_range_idx_ = -1;
  io_micro_infos_.reuse();
  micro_info_iter_.reuse();
  block_index_handle_mgr_.reset();
  block_handle_mgr_.reset();
  table_store_stat_.reuse();
  skip_ctx_.reset();
  storage_file_ = nullptr;
  prefetch_handle_depth_ = DEFAULT_PREFETCH_HANDLE_DEPTH;
  prefetch_micro_depth_ = DEFAULT_PREFETCH_MICRO_DEPTH;
}

int ObSSTableRowIterator::get_read_handle(const ObExtStoreRowkey& ext_rowkey, ObSSTableReadHandle& read_handle)
{
  int ret = OB_SUCCESS;
  bool found = false;
  ObQueryFlag query_flag = access_ctx_->query_flag_;
  bool skip_macro_bf = false;
  read_handle.reset();

  read_handle.is_get_ = true;
  read_handle.ext_rowkey_ = &ext_rowkey;

  // check if sstable is empty
  if (0 == sstable_->get_macro_block_count()) {
    // empty sstable
    found = true;
    read_handle.state_ = ObSSTableRowState::NOT_EXIST;
  }

  if (OB_SUCC(ret) && !found && access_ctx_->enable_sstable_bf_cache() &&
      sstable_->get_rowkey_column_count() == ext_rowkey.get_store_rowkey().get_obj_cnt()) {
    if (sstable_->has_bloom_filter_macro_block()) {
      bool is_contain = true;
      if (OB_FAIL(sstable_->bf_may_contain_rowkey(ext_rowkey.get_store_rowkey(), is_contain))) {
        if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
          STORAGE_LOG(WARN, "fail to check if rowkey may contain in sstable bloomfilter", K(ret), K(ext_rowkey));
        }
        ret = OB_SUCCESS;
      } else {
        if (!is_contain) {
          found = true;
          read_handle.state_ = ObSSTableRowState::NOT_EXIST;
          ++access_ctx_->access_stat_.sstable_bf_filter_cnt_;
          ++access_ctx_->access_stat_.bf_filter_cnt_;
          ++table_store_stat_.bf_filter_cnt_;
        }
        skip_macro_bf = true;
      }
      access_ctx_->access_stat_.rowkey_prefix_ = ext_rowkey.get_store_rowkey().get_obj_cnt();
      ++access_ctx_->access_stat_.sstable_bf_access_cnt_;
      ++access_ctx_->access_stat_.bf_access_cnt_;
      ++table_store_stat_.bf_access_cnt_;
    }
  }

  // try get from row cache
  if (OB_SUCC(ret) && !found && access_ctx_->enable_get_row_cache()) {
    ObRowCacheKey key(iter_param_->table_id_,
        storage_file_->get_file_id(),
        ext_rowkey.get_store_rowkey(),
        sstable_snapshot_version_,
        table_type_);
    if (OB_FAIL(ObStorageCacheSuite::get_instance().get_row_cache().get_row(key, read_handle.row_handle_))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "Fail to get row from row cache, ", K(ret), K(key));
      } else {
        ++access_ctx_->access_stat_.row_cache_miss_cnt_;
        ++table_store_stat_.row_cache_miss_cnt_;
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(
                   sstable_->get_meta(read_handle.row_handle_.row_value_->get_block_id(), read_handle.full_meta_))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "Fail to get macro block meta, ", K(ret), K(read_handle));
      } else {
        ++access_ctx_->access_stat_.row_cache_miss_cnt_;
        ++table_store_stat_.row_cache_miss_cnt_;
        ret = OB_SUCCESS;
      }
    } else {
      found = true;
      read_handle.state_ = ObSSTableRowState::IN_ROW_CACHE;
      ++access_ctx_->access_stat_.row_cache_hit_cnt_;
      ++table_store_stat_.row_cache_hit_cnt_;
    }
  }

  // try find macro block id
  if (OB_SUCC(ret) && !found) {
    if (OB_FAIL(macro_block_iter_.open(*sstable_, ext_rowkey))) {
      STORAGE_LOG(WARN, "Fail to open macro block iter, ", K(ret), K(ext_rowkey));
    } else if (OB_FAIL(macro_block_iter_.get_next_macro_block(read_handle.macro_block_ctx_, read_handle.full_meta_))) {
      if (OB_ITER_END == ret) {
        // not exist rowkey
        ret = OB_SUCCESS;
        found = true;
        read_handle.state_ = ObSSTableRowState::NOT_EXIST;
      } else {
        STORAGE_LOG(WARN, "Fail to get next macro block, ", K(ret));
      }
    } else {
      STORAGE_LOG(DEBUG, "Get macro block, ", K(ext_rowkey), K(read_handle));
    }
  }

  // try bloom filter
  if (OB_SUCC(ret) && !found && !skip_macro_bf && read_handle.full_meta_.is_valid()) {
    if (!query_flag.is_index_back() && access_ctx_->enable_bf_cache()) {
      bool is_contain = true;
      if (OB_FAIL(ObStorageCacheSuite::get_instance().get_bf_cache().may_contain(iter_param_->table_id_,
              read_handle.macro_block_ctx_.get_macro_block_id(),
              storage_file_->get_file_id(),
              ext_rowkey.get_store_rowkey(),
              is_contain))) {
        if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
          STORAGE_LOG(WARN, "fail to check if rowkey may contain in bloomfilter", K(ret), K(ext_rowkey));
        }
        ret = OB_SUCCESS;
      } else {
        if (!is_contain) {
          found = true;
          read_handle.state_ = ObSSTableRowState::NOT_EXIST;
          ++access_ctx_->access_stat_.bf_filter_cnt_;
          ++table_store_stat_.bf_filter_cnt_;
        } else {
          read_handle.is_bf_contain_ = true;
        }
      }
      access_ctx_->access_stat_.rowkey_prefix_ = ext_rowkey.get_store_rowkey().get_obj_cnt();
      ++access_ctx_->access_stat_.bf_access_cnt_;
      ++table_store_stat_.bf_access_cnt_;
    }
  }

  // try get block index from cache
  if (OB_SUCC(ret) && !found) {
    read_handle.state_ = ObSSTableRowState::IN_BLOCK;
  }

  return ret;
}

int ObSSTableRowIterator::prefetch()
{
  int ret = OB_SUCCESS;
  int64_t prefetch_handle_cnt = 0;
  int64_t prefetching_handle_cnt = cur_prefetch_handle_pos_ - cur_read_handle_pos_;
  int64_t sstable_micro_cnt = 0;
  int64_t prefetch_micro_cnt = 0;
  int64_t total_sstable_micro_cnt = 0;
  int64_t prefetching_micro_cnt = cur_prefetch_micro_pos_ - cur_read_micro_pos_;
  int64_t prefetching_micro_handle_cnt = cur_fetch_handle_pos_ - cur_read_handle_pos_;

  if (!prefetch_block_end_) {
    if (!prefetch_handle_end_) {
      // prefetch read handle
      if (prefetching_handle_cnt <= read_handle_cnt_ / 2 && prefetching_handle_cnt <= prefetch_handle_depth_ / 4) {
        prefetch_handle_cnt = std::min(read_handle_cnt_ - prefetching_handle_cnt, prefetch_handle_depth_);
        prefetch_handle_depth_ = min(read_handle_cnt_, prefetch_handle_depth_ * 2);
      }
      if (OB_FAIL(prefetch_handle(prefetch_handle_cnt))) {
        STORAGE_LOG(WARN, "Fail to prefetch handle, ", K(ret), K(prefetch_handle_cnt));
      }
    }

    if (OB_SUCC(ret)) {
      if ((prefetching_micro_cnt <= micro_handle_cnt_ / 2 && prefetching_micro_cnt <= prefetch_micro_depth_ / 4) ||
          0 == prefetching_micro_handle_cnt || 0 == prefetching_micro_cnt) {
        // prefetching micro count is less than free micro count and prefetch micro depth
        prefetch_micro_cnt = std::min(micro_handle_cnt_ - prefetching_micro_cnt, prefetch_micro_depth_);
        prefetch_micro_depth_ = min(micro_handle_cnt_, prefetch_micro_depth_ * 2);
      }
    }
    STORAGE_LOG(DEBUG,
        "prefetch info",
        K(prefetching_micro_handle_cnt),
        K(prefetching_micro_cnt),
        K(prefetch_micro_depth_),
        K(prefetch_micro_cnt),
        K(prefetch_handle_depth_),
        K(prefetch_handle_cnt),
        K(read_handle_cnt_),
        K(micro_handle_cnt_));

    while (OB_SUCC(ret) && total_sstable_micro_cnt < prefetch_micro_cnt) {
      if (OB_FAIL(micro_info_iter_.get_next_micro(sstable_micro_infos_[sstable_micro_cnt]))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          if ((micro_handle_cnt_ >= LIMIT_PREFETCH_BLOCK_CACHE_THRESHOLD &&
                  cur_fetch_handle_pos_ > (cur_prefetch_handle_pos_ + cur_read_handle_pos_) / 2)) {
            break;
          } else if (cur_fetch_handle_pos_ >= cur_prefetch_handle_pos_) {
            if (prefetch_handle_end_) {
              prefetch_block_end_ = true;
            }
            break;
          } else if (OB_FAIL(micro_info_iter_.open(this, read_handles_[cur_fetch_handle_pos_ % read_handle_cnt_]))) {
            STORAGE_LOG(WARN,
                "Fail to open read handle, ",
                K(ret),
                K(sstable_micro_cnt),
                K_(cur_prefetch_handle_pos),
                K_(cur_fetch_handle_pos),
                K_(cur_read_handle_pos));
          } else {
            ++cur_fetch_handle_pos_;
          }
        } else {
          STORAGE_LOG(WARN, "Fail to get next sstable micro info, ", K(ret));
        }
      } else {
        sorted_sstable_micro_infos_[sstable_micro_cnt] = sstable_micro_infos_[sstable_micro_cnt];
        sstable_micro_cnt += sstable_micro_infos_[sstable_micro_cnt].is_skip_ ? 0 : 1;
        total_sstable_micro_cnt++;
      }
    }

    // prefetch micro block
    if (OB_SUCC(ret)) {
      if (sstable_micro_cnt > 0) {
        if (OB_FAIL(prefetch_block(sstable_micro_cnt))) {
          STORAGE_LOG(WARN, "Fail to prefetch block, ", K(ret));
        } else {
          STORAGE_LOG(DEBUG,
              "Success to prefetch block, ",
              K(sstable_micro_cnt),
              K_(cur_read_handle_pos),
              K_(cur_fetch_handle_pos),
              K_(cur_prefetch_handle_pos));
        }
      }
    }
  }
  return ret;
}

int ObSSTableRowIterator::prefetch_handle(const int64_t prefetch_handle_cnt)
{
  int ret = OB_SUCCESS;
  int64_t prefetched_cnt = 0;
  if (prefetch_handle_cnt > 0) {
    while (OB_SUCC(ret) && prefetched_cnt < prefetch_handle_cnt) {
      ObSSTableReadHandle& prefetch_handle = read_handles_[cur_prefetch_handle_pos_ % read_handle_cnt_];
      if (OB_FAIL(prefetch_read_handle(prefetch_handle))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "Fail to prefetch read handle, ", K(ret));
        } else {
          prefetch_handle_end_ = true;
          ret = OB_SUCCESS;
          break;
        }
      } else {
        if (ObSSTableRowState::IN_BLOCK == prefetch_handle.state_) {
          if (sstable_->is_rowkey_helper_valid()) {
            prefetch_handle.rowkey_helper_ = &(sstable_->get_rowkey_helper());
          }
          ObMicroBlockIndexHandle* handle = NULL;
          if (OB_FAIL(prefetch_handle.get_index_handle(handle))) {
            STORAGE_LOG(WARN, "Fail to get index handle, ", K(ret), K(prefetch_handle));
          } else if (OB_FAIL(block_index_handle_mgr_.get_block_index_handle(iter_param_->table_id_,
                         prefetch_handle.macro_block_ctx_,
                         storage_file_->get_file_id(),
                         access_ctx_->query_flag_,
                         storage_file_,
                         *handle))) {
            STORAGE_LOG(WARN, "Fail to get block index handle, ", K(ret));
          }
        }
        ++cur_prefetch_handle_pos_;
        ++prefetched_cnt;
      }
    }
  }
  return ret;
}

int ObSSTableRowIterator::prefetch_block(const int64_t sstable_micro_cnt)
{
  int ret = OB_SUCCESS;
  ObMacroBlockCtx last_macro_ctx;
  ObMultiBlockIOParam io_param;
  int64_t start_index = 0;
  int32_t prev_offset = 0;
  int32_t gap_size = 0;
  int32_t read_size = 0;
  int64_t multiblock_read_gap_size_threshold = GCONF.multiblock_read_gap_size.get_value();
  int64_t multiblock_read_size_threshold = GCONF.multiblock_read_size.get_value();
  bool use_multiblock_io = false;

  if (sstable_micro_cnt > 0) {
    // sort micro info
    cur_prefetch_micro_pos_ = sstable_micro_infos_[sstable_micro_cnt - 1].micro_idx_ + 1;
    std::sort(
        &sorted_sstable_micro_infos_[0], &sorted_sstable_micro_infos_[sstable_micro_cnt], ObSSTableMicroBlockInfoCmp());
    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_micro_cnt; ++i) {
      const ObSSTableMicroBlockInfo& sstable_micro = sstable_micro_infos_[i];
      if (last_macro_ctx.get_macro_block_id() == sstable_micro.macro_ctx_.get_macro_block_id()) {
        read_size += sstable_micro.micro_info_.size_ + (sstable_micro.micro_info_.offset_ - prev_offset);
        prev_offset = sstable_micro.micro_info_.offset_ + sstable_micro.micro_info_.size_;
        if (read_size > multiblock_read_size_threshold) {
          use_multiblock_io = true;
          break;
        }
      } else {
        read_size = sstable_micro.micro_info_.size_;
        prev_offset = sstable_micro.micro_info_.offset_ + sstable_micro.micro_info_.size_;
        last_macro_ctx = sstable_micro.macro_ctx_;
      }
    }
    if (!use_multiblock_io) {
      multiblock_read_gap_size_threshold = 0;
      multiblock_read_size_threshold = 0;
    }

    // group prefetch block
    io_micro_infos_.reuse();
    io_param.micro_block_infos_ = &io_micro_infos_;
    io_param.start_index_ = 0;
    io_param.block_count_ = 0;
    prev_offset = 0;
    read_size = 0;
    last_macro_ctx.reset();
    MicroInfoArray& sstable_micro_infos = use_multiblock_io ? sorted_sstable_micro_infos_ : sstable_micro_infos_;

    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_micro_cnt; ++i) {
      const ObSSTableMicroBlockInfo& sstable_micro = sstable_micro_infos[i];
      ObMicroBlockDataHandle& micro_handle = micro_handles_[sstable_micro.micro_idx_ % micro_handle_cnt_];
      bool need_submit_io = false;
      if (OB_FAIL(block_handle_mgr_.get_micro_block_handle(iter_param_->table_id_,
              sstable_micro.macro_ctx_,
              storage_file_->get_file_id(),
              sstable_micro.micro_info_.offset_,
              sstable_micro.micro_info_.size_,
              sstable_micro.micro_info_.index_,
              micro_handle))) {
        // cache miss
        ++access_ctx_->access_stat_.block_cache_miss_cnt_;
        ++table_store_stat_.block_cache_miss_cnt_;
        ret = OB_SUCCESS;
        if (io_param.block_count_ > 0) {
          STORAGE_LOG(DEBUG,
              "current micro info",
              K(last_macro_ctx.get_macro_block_id()),
              K(sstable_micro.macro_ctx_.get_macro_block_id()),
              K(sstable_micro.micro_info_));
          if (last_macro_ctx.get_macro_block_id() == sstable_micro.macro_ctx_.get_macro_block_id()) {
            // same macro block
            if (sstable_micro.micro_info_.offset_ >= prev_offset) {
              // different micro block
              gap_size += sstable_micro.micro_info_.offset_ - prev_offset;
              read_size += sstable_micro.micro_info_.size_ + (sstable_micro.micro_info_.offset_ - prev_offset);
              prev_offset = sstable_micro.micro_info_.offset_ + sstable_micro.micro_info_.size_;
              if (gap_size > multiblock_read_gap_size_threshold || read_size > multiblock_read_size_threshold) {
                need_submit_io = true;
              } else {
                if (OB_FAIL(io_micro_infos_.push_back(sstable_micro.micro_info_))) {
                  STORAGE_LOG(WARN, "Fail to push micro info to sstable micros, ", K(ret));
                } else {
                  io_param.block_count_++;
                }
              }
            } else {
              need_submit_io = true;
            }
          } else {
            // different macro block
            need_submit_io = true;
          }
        }

        if (OB_SUCC(ret) && need_submit_io) {
          if (OB_FAIL(submit_block_io(io_param, sstable_micro_infos, start_index, i - 1))) {
            STORAGE_LOG(WARN, "Fail to submit block io, ", K(ret), K(start_index), K(i));
          } else {
            io_param.block_count_ = 0;
          }
        }

        if (OB_SUCC(ret) && 0 == io_param.block_count_) {
          // empty io param
          if (OB_FAIL(io_micro_infos_.push_back(sstable_micro.micro_info_))) {
            STORAGE_LOG(WARN, "Fail to push micro info to sstable micros, ", K(ret));
          } else {
            io_param.block_count_++;
            gap_size = 0;
            read_size = sstable_micro.micro_info_.size_;
            prev_offset = sstable_micro.micro_info_.offset_ + sstable_micro.micro_info_.size_;
            start_index = i;
            last_macro_ctx = sstable_micro.macro_ctx_;
          }
        }
      } else {
        // cache hit
        ++access_ctx_->access_stat_.block_cache_hit_cnt_;
        ++table_store_stat_.block_cache_hit_cnt_;
      }
      if (OB_SUCC(ret)) {
        micro_handle.micro_info_ = sstable_micro.micro_info_;
      }
    }

    if (OB_SUCC(ret) && io_param.block_count_ > 0) {
      if (OB_FAIL(submit_block_io(io_param, sstable_micro_infos, start_index, sstable_micro_cnt - 1))) {
        STORAGE_LOG(WARN, "Fail to submit block io, ", K(ret), K(start_index), K(sstable_micro_cnt));
      }
    }
  }
  return ret;
}

int ObSSTableRowIterator::submit_block_io(ObMultiBlockIOParam& io_param, MicroInfoArray& sstable_micro_infos,
    const int64_t start_sstable_micro_idx, const int64_t end_sstable_micro_idx)
{
  int ret = OB_SUCCESS;
  ObMacroBlockHandle macro_handle;
  int32_t block_index = 0;

  if (io_param.block_count_ > 0) {
    macro_handle.set_file(storage_file_);
    ObMacroBlockCtx& block_ctx = sstable_micro_infos[start_sstable_micro_idx].macro_ctx_;
    if (1 == io_param.block_count_) {
      if (OB_FAIL(block_cache_->prefetch(iter_param_->table_id_,
              block_ctx,
              io_param.micro_block_infos_->at(io_param.start_index_).offset_,
              io_param.micro_block_infos_->at(io_param.start_index_).size_,
              access_ctx_->query_flag_,
              storage_file_,
              macro_handle))) {
        STORAGE_LOG(WARN, "Fail to prefetch micro block, ", K(ret));
      }
    } else {
      if (OB_FAIL(block_cache_->prefetch(
              iter_param_->table_id_, block_ctx, io_param, access_ctx_->query_flag_, storage_file_, macro_handle))) {
        STORAGE_LOG(WARN, "Fail to prefetch multi io block, ", K(ret));
      }
    }

    STORAGE_LOG(DEBUG, "Submit block io, ", K(block_ctx), K(io_param));

    if (OB_SUCC(ret)) {
      const ObSSTableMicroBlockInfo* last_sstable_micro = NULL;
      for (int64_t i = start_sstable_micro_idx; i <= end_sstable_micro_idx; ++i) {
        const ObSSTableMicroBlockInfo& sstable_micro = sstable_micro_infos[i];
        ObMicroBlockDataHandle& micro_handle = micro_handles_[sstable_micro.micro_idx_ % micro_handle_cnt_];
        if (ObSSTableMicroBlockState::UNKNOWN_STATE == micro_handle.block_state_) {
          micro_handle.table_id_ = iter_param_->table_id_;
          micro_handle.block_ctx_ = block_ctx;
          micro_handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_IO;
          micro_handle.io_handle_ = macro_handle;

          if (1 == io_param.block_count_) {
            micro_handle.block_index_ = -1;
          } else {
            if (NULL == last_sstable_micro) {
              micro_handle.block_index_ = 0;
            } else {
              if (last_sstable_micro->micro_info_.offset_ != sstable_micro.micro_info_.offset_) {
                block_index++;
              }
              micro_handle.block_index_ = block_index;
            }
            last_sstable_micro = &sstable_micro;
          }
        }
      }

      io_param.micro_block_infos_->reuse();
      io_param.start_index_ = 0;
      io_param.block_count_ = 0;
    }
  }
  return ret;
}

int ObSSTableRowIterator::exist_row(ObSSTableReadHandle& read_handle, ObStoreRow& store_row)
{
  int ret = OB_SUCCESS;
  switch (read_handle.state_) {
    case ObSSTableRowState::NOT_EXIST:
      store_row.flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
      break;
    case ObSSTableRowState::IN_ROW_CACHE:
      store_row.flag_ = read_handle.row_handle_.row_value_->get_flag();
      break;
    case ObSSTableRowState::IN_BLOCK:
      if (OB_FAIL(exist_block_row(read_handle, store_row))) {
        STORAGE_LOG(WARN, "Fail to check row exist in block, ", K(ret));
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid row state", K(ret), K(read_handle.state_));
  }
  if (OB_SUCC(ret)) {
    store_row.scan_index_ = read_handle.range_idx_;
    store_row.is_get_ = true;
    ++cur_read_handle_pos_;
    STORAGE_LOG(
        DEBUG, "get exist row", K(read_handle.state_), K(read_handle.ext_rowkey_->get_store_rowkey()), KP(this));
  }
  return ret;
}

int ObSSTableRowIterator::get_block_data(const int64_t micro_block_idx, ObMicroBlockData& block_data)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(prefetch())) {
    STORAGE_LOG(WARN, "Fail to prefetch data, ", K(ret));
  } else if (OB_UNLIKELY(micro_block_idx >= cur_prefetch_micro_pos_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN,
        "The read micro block idx has exceed prefetch micro pos, ",
        K(ret),
        K(micro_block_idx),
        K_(cur_prefetch_micro_pos));
  } else if (OB_FAIL(micro_handles_[micro_block_idx % micro_handle_cnt_].get_block_data(
                 block_reader_, storage_file_, block_data))) {
    STORAGE_LOG(WARN, "Fail to get block data, ", K(ret), K(micro_block_idx));
  } else {
    cur_read_micro_pos_ = micro_block_idx;
  }
  return ret;
}

int ObSSTableRowIterator::exist_block_row(ObSSTableReadHandle& read_handle, ObStoreRow& store_row)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData block_data;
  bool exist = false;
  bool found = false;

  if (NULL == micro_exister_) {
    if (NULL == (micro_exister_ = OB_NEWx(ObMicroBlockRowExister, access_ctx_->allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Fail to allocate micro exister, ", K(ret));
    } else if (OB_FAIL(micro_exister_->init(*iter_param_, *access_ctx_, sstable_))) {
      STORAGE_LOG(WARN, "Fail to init micro exister, ", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    for (int64_t i = read_handle.micro_begin_idx_; OB_SUCC(ret) && !found && i <= read_handle.micro_end_idx_; ++i) {
      if (OB_FAIL(get_block_data(i, block_data))) {
        STORAGE_LOG(WARN, "Fail to get block data, ", K(ret), K(read_handle));
      } else if (OB_FAIL(micro_exister_->is_exist(read_handle.ext_rowkey_->get_store_rowkey(),
                     read_handle.full_meta_,
                     block_data,
                     read_handle.rowkey_helper_,
                     exist,
                     found))) {
        STORAGE_LOG(WARN, "Fail to get row, ", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (!found) {
      store_row.flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
      if (!access_ctx_->query_flag_.is_index_back() && access_ctx_->query_flag_.is_use_bloomfilter_cache()) {
        (void)ObStorageCacheSuite::get_instance().get_bf_cache().inc_empty_read(iter_param_->table_id_,
            read_handle.get_macro_block_id(),
            storage_file_->get_file_id(),
            *read_handle.full_meta_.meta_,
            read_handle.ext_rowkey_->get_store_rowkey().get_obj_cnt(),
            sstable_->get_key());
        if (read_handle.is_bf_contain_) {
          ++table_store_stat_.bf_empty_read_cnt_;
        }
      }
      ++access_ctx_->access_stat_.empty_read_cnt_;
      ++table_store_stat_.exist_row_.empty_read_cnt_;
      EVENT_INC(ObStatEventIds::EXIST_ROW_EMPTY_READ);
    } else {
      if (exist) {
        store_row.flag_ = ObActionFlag::OP_ROW_EXIST;
      } else {
        store_row.flag_ = ObActionFlag::OP_DEL_ROW;
      }
      ++table_store_stat_.exist_row_.effect_read_cnt_;
      EVENT_INC(ObStatEventIds::EXIST_ROW_EFFECT_READ);
    }
  }
  return ret;
}

int ObSSTableRowIterator::get_row(ObSSTableReadHandle& read_handle, const ObStoreRow*& store_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(alloc_micro_getter())) {
    STORAGE_LOG(WARN, "Fail to alloc micro getter, ", K(ret));
  } else {
    switch (read_handle.state_) {
      case ObSSTableRowState::NOT_EXIST:
        if (OB_FAIL(micro_getter_->get_not_exist_row(read_handle.ext_rowkey_->get_store_rowkey(), store_row))) {
          STORAGE_LOG(WARN, "fail to get not exist row", K(ret), K(*read_handle.ext_rowkey_));
        }
        break;
      case ObSSTableRowState::IN_ROW_CACHE:
        if (OB_FAIL(micro_getter_->get_cached_row(read_handle.ext_rowkey_->get_store_rowkey(),
                read_handle.full_meta_,
                *read_handle.row_handle_.row_value_,
                store_row))) {
          STORAGE_LOG(WARN, "fail to get cache row", K(ret), K(*read_handle.ext_rowkey_));
        }
        break;
      case ObSSTableRowState::IN_BLOCK:
        if (OB_FAIL(get_block_row(read_handle, store_row))) {
          STORAGE_LOG(WARN, "fail to get block row", K(ret), K(*read_handle.ext_rowkey_));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "invalid row state", K(ret), K(read_handle.state_));
    }
  }

  if (OB_SUCC(ret)) {
    (const_cast<ObStoreRow*>(store_row))->scan_index_ = read_handle.range_idx_;
    (const_cast<ObStoreRow*>(store_row))->is_get_ = true;
    ++cur_read_handle_pos_;
    STORAGE_LOG(DEBUG,
        "get row",
        K(*store_row),
        K(read_handle.state_),
        K(read_handle.ext_rowkey_->get_store_rowkey()),
        KP(this));
  }
  return ret;
}

int ObSSTableRowIterator::get_block_row(ObSSTableReadHandle& read_handle, const ObStoreRow*& store_row)
{
  int ret = OB_SUCCESS;
  store_row = NULL;
  ObMicroBlockData block_data;
  bool found = false;
  int64_t row_cache_put_cnt = access_ctx_->access_stat_.row_cache_put_cnt_;
  for (int64_t i = read_handle.micro_begin_idx_; OB_SUCC(ret) && !found && i <= read_handle.micro_end_idx_; ++i) {
    if (OB_FAIL(get_block_data(i, block_data))) {
      STORAGE_LOG(WARN, "Fail to get block data, ", K(ret), K(read_handle));
    } else if (OB_FAIL(micro_getter_->get_row(read_handle.ext_rowkey_->get_store_rowkey(),
                   read_handle.get_macro_block_id(),
                   storage_file_->get_file_id(),
                   read_handle.full_meta_,
                   block_data,
                   read_handle.rowkey_helper_,
                   store_row))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "Fail to get row, ", K(ret));
      }
    } else {
      if (ObActionFlag::OP_ROW_DOES_NOT_EXIST != store_row->flag_) {
        found = true;
      }
    }
  }
  table_store_stat_.row_cache_put_cnt_ += (access_ctx_->access_stat_.row_cache_put_cnt_ - row_cache_put_cnt);

  // if row does not exist in sstable, get a not exist row
  if (OB_SUCC(ret) && !found) {
    ++table_store_stat_.get_row_.empty_read_cnt_;
    ++access_ctx_->access_stat_.empty_read_cnt_;
    EVENT_INC(ObStatEventIds::GET_ROW_EMPTY_READ);
    if (OB_FAIL(micro_getter_->get_not_exist_row(read_handle.ext_rowkey_->get_store_rowkey(), store_row))) {
      STORAGE_LOG(WARN, "fail to get not exist row", K(ret), K(read_handle));
    }
    if (!access_ctx_->query_flag_.is_index_back() && access_ctx_->query_flag_.is_use_bloomfilter_cache()) {
      // ignore ret
      (void)ObStorageCacheSuite::get_instance().get_bf_cache().inc_empty_read(iter_param_->table_id_,
          read_handle.get_macro_block_id(),
          storage_file_->get_file_id(),
          *read_handle.full_meta_.meta_,
          iter_param_->rowkey_cnt_,
          sstable_->get_key());
      if (read_handle.is_bf_contain_) {
        ++table_store_stat_.bf_empty_read_cnt_;
      }
    }
  } else {
    ++table_store_stat_.get_row_.effect_read_cnt_;
    EVENT_INC(ObStatEventIds::GET_ROW_EFFECT_READ);
  }

  return ret;
}

int ObSSTableRowIterator::scan_row(ObSSTableReadHandle& read_handle, const ObStoreRow*& store_row)
{
  int ret = OB_SUCCESS;
  ObSSTableSkipRangeCtx* skip_ctx = NULL;
  if (ObSSTableRowState::NOT_EXIST == read_handle.state_) {
    // empty range
    ret = OB_ITER_END;
  } else if (OB_FAIL(get_skip_range_ctx(read_handle, INT64_MAX, skip_ctx))) {
    STORAGE_LOG(WARN, "fail to check need skip range", K(ret));
  } else if ((NULL != skip_ctx && skip_ctx->need_skip_) || read_handle.micro_begin_idx_ > read_handle.micro_end_idx_) {
    // skip current macro block
    ret = OB_ITER_END;
  } else {
    bool is_first_open = false;
    bool is_new_skip_range = false;
    bool need_open_micro = false;
    if (-1 == cur_micro_idx_ || cur_micro_idx_ < read_handle.micro_begin_idx_) {
      is_first_open = -1 == cur_micro_idx_;
      is_new_skip_range = !is_first_open && cur_micro_idx_ < read_handle.micro_begin_idx_;
      cur_micro_idx_ = read_handle.micro_begin_idx_;
      need_open_micro = true;
      STORAGE_LOG(DEBUG, "switch to read handle first micro block", K(read_handle));
    }

    // check need skip micro block
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(cur_micro_idx_ >= cur_prefetch_micro_pos_)) {
        ret = OB_EAGAIN;
        cur_read_micro_pos_ = cur_prefetch_micro_pos_;
      } else if (OB_FAIL(get_skip_range_ctx(read_handle, cur_micro_idx_, skip_ctx))) {
        STORAGE_LOG(WARN, "fail to check need skip range", K(ret));
      } else if (NULL == skip_ctx) {
        // do nothing
      } else if (skip_ctx->need_skip_) {
        ret = OB_EAGAIN;
        ++cur_micro_idx_;
        ++cur_read_micro_pos_;
      } else if (skip_ctx->micro_idx_ == cur_micro_idx_ && !skip_ctx->is_micro_reopen_) {
        is_new_skip_range = true;
        STORAGE_LOG(DEBUG,
            "open micro block read handle",
            K(*read_handle.ext_range_),
            K(read_handle),
            K(cur_micro_idx_),
            K(*skip_ctx));
        if (read_handle.micro_begin_idx_ != cur_micro_idx_) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN,
              "error unexpected, read handle micro_begin_idx is invalid",
              K(ret),
              K(read_handle),
              K(*skip_ctx),
              K(cur_micro_idx_));
        } else {
          need_open_micro = true;
        }
      }
    }

    if (OB_SUCC(ret) && need_open_micro) {
      if (OB_FAIL(open_cur_micro_block(read_handle, is_new_skip_range))) {
        STORAGE_LOG(WARN, "Fail to open micro block, ", K(ret), K_(cur_micro_idx));
      } else if (NULL != skip_ctx && skip_ctx->micro_idx_ == cur_micro_idx_) {
        skip_ctx->is_micro_reopen_ = true;
      }
    }
    // TODO: fast skip pass filter to first batch row scanner
    while (OB_SUCC(ret)) {
      if (OB_FAIL(batch_get_next_row(micro_scanner_, store_row))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "Fail to get next row, ", K(ret));
        } else {
          ret = OB_SUCCESS;
          ++cur_micro_idx_;
          if (cur_micro_idx_ <= read_handle.micro_end_idx_) {
            if (OB_FAIL(open_cur_micro_block(read_handle))) {
              STORAGE_LOG(WARN, "Fail to open micro block, ", K(ret), K_(cur_micro_idx), K(read_handle));
            }
          } else {
            cur_micro_idx_ = -1;
            ret = OB_ITER_END;
          }
        }
      } else {
        (const_cast<ObStoreRow*>(store_row))->scan_index_ = read_handle.range_idx_;
        (const_cast<ObStoreRow*>(store_row))->is_get_ = false;
        if (store_row->row_pos_flag_.is_micro_first()) {
          int64_t micro_idx = -1;
          if (OB_LIKELY(OB_SUCCESS == get_cur_micro_idx_in_macro(micro_idx)) && 0 == micro_idx) {
            (const_cast<ObStoreRow*>(store_row))->row_pos_flag_.set_macro_first(true);
          }
        }
        break;
      }
    }

    if (OB_ITER_END == ret && is_first_open && read_handle.is_left_border_ && read_handle.is_right_border_ &&
        !is_new_skip_range) {
      ++table_store_stat_.scan_row_.empty_read_cnt_;
      ++access_ctx_->access_stat_.empty_read_cnt_;
      EVENT_INC(ObStatEventIds::SCAN_ROW_EMPTY_READ);
      // single macro block empty read, ignore ret
      int64_t common_prefix_len = 0;
      if (access_ctx_->query_flag_.is_use_bloomfilter_cache() &&
          OB_SUCCESS == ObStoreRowkey::get_common_prefix_length(read_handle.ext_range_->get_range().get_start_key(),
                            read_handle.ext_range_->get_range().get_end_key(),
                            common_prefix_len)) {
        if (common_prefix_len > 0) {
          (void)ObStorageCacheSuite::get_instance().get_bf_cache().inc_empty_read(iter_param_->table_id_,
              read_handle.get_macro_block_id(),
              storage_file_->get_file_id(),
              *read_handle.full_meta_.meta_,
              common_prefix_len,
              sstable_->get_key());
          if (read_handle.is_bf_contain_) {
            ++table_store_stat_.bf_empty_read_cnt_;
          }
        }
      }
    } else {
      ++table_store_stat_.scan_row_.effect_read_cnt_;
      EVENT_INC(ObStatEventIds::SCAN_ROW_EFFECT_READ);
    }
  }

  return ret;
}

int ObSSTableRowIterator::get_cur_read_handle(ObSSTableReadHandle*& read_handle)
{
  int ret = OB_SUCCESS;
  read_handle = NULL;
  if (OB_UNLIKELY(cur_read_handle_pos_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(cur_read_handle_pos_));
  } else {
    read_handle = &read_handles_[cur_read_handle_pos_ % read_handle_cnt_];
  }
  return ret;
}

int ObSSTableRowIterator::get_cur_read_micro_handle(ObMicroBlockDataHandle*& micro_handle)
{
  int ret = OB_SUCCESS;
  micro_handle = NULL;
  if (OB_UNLIKELY(!is_opened_ || cur_micro_idx_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(is_opened_), K(cur_micro_idx_));
  } else {
    micro_handle = &micro_handles_[cur_micro_idx_ % micro_handle_cnt_];
  }
  return ret;
}

int ObSSTableRowIterator::get_cur_micro_idx_in_macro(int64_t& micro_idx)
{
  int ret = OB_SUCCESS;
  micro_idx = -1;
  if (OB_UNLIKELY(!is_opened_ || cur_micro_idx_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(is_opened_), K(cur_micro_idx_));
  } else {
    ObMicroBlockDataHandle& micro_handle = micro_handles_[cur_micro_idx_ % micro_handle_cnt_];
    if (OB_UNLIKELY(!micro_handle.micro_info_.is_valid())) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "invalid micro info", K(ret), K(micro_handle.micro_info_));
    } else {
      micro_idx = micro_handle.micro_info_.index_;
    }
  }
  return ret;
}

int ObSSTableRowIterator::get_cur_micro_row_count(int64_t& row_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(micro_scanner_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "micro_scanner_ is null");
  } else if (OB_FAIL(micro_scanner_->get_cur_micro_row_count(row_count))) {
    STORAGE_LOG(WARN, "ObSSTableRowIterator get micro row count failed", K(ret));
  }
  return ret;
}
int ObSSTableRowIterator::alloc_micro_getter()
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  if (NULL == micro_getter_) {
    if (NULL == (buf = access_ctx_->allocator_->alloc(sizeof(ObMicroBlockRowGetter)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
    } else {
      micro_getter_ = new (buf) ObMicroBlockRowGetter();
      if (OB_FAIL(micro_getter_->init(*iter_param_, *access_ctx_, sstable_))) {
        STORAGE_LOG(WARN, "Fail to init micro block row getter, ", K(ret));
      }
    }
  }
  return ret;
}

int ObSSTableRowIterator::open_cur_micro_block(ObSSTableReadHandle& read_handle, const bool is_new_skip_range)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData block_data;
  void* buf = NULL;
  bool is_left_border = false;
  bool is_right_border = false;
  const int64_t micro_block_idx = cur_micro_idx_;

  if (access_ctx_->query_flag_.is_reverse_scan()) {
    is_left_border = read_handle.is_left_border_ && micro_block_idx == read_handle.micro_end_idx_;
    is_right_border = read_handle.is_right_border_ && micro_block_idx == read_handle.micro_begin_idx_;
  } else {
    is_left_border = read_handle.is_left_border_ && micro_block_idx == read_handle.micro_begin_idx_;
    is_right_border = read_handle.is_right_border_ && micro_block_idx == read_handle.micro_end_idx_;
  }

  if (NULL == micro_scanner_) {
    // alloc scanner
    if (!sstable_->is_multi_version_minor_sstable()) {
      if (NULL == (buf = access_ctx_->allocator_->alloc(sizeof(ObMicroBlockRowScanner)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Fail to allocate memory for micro block scanner, ", K(ret));
      } else {
        micro_scanner_ = new (buf) ObMicroBlockRowScanner();
      }
    } else {
      if (NULL == (buf = access_ctx_->allocator_->alloc(sizeof(ObMultiVersionMicroBlockRowScanner)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Fail to allocate memory for micro block scanner, ", K(ret));
      } else {
        micro_scanner_ = new (buf) ObMultiVersionMicroBlockRowScanner();
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(micro_scanner_->init(*iter_param_, *access_ctx_, sstable_))) {
        STORAGE_LOG(WARN, "Fail to init micro scanner, ", K(ret), K(read_handle));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // init scanner
    if (micro_block_idx == read_handle.micro_begin_idx_ &&
        ((cur_range_idx_ != read_handle.range_idx_) || is_new_skip_range)) {
      micro_scanner_->rescan();
      if (OB_FAIL(micro_scanner_->set_range(read_handle.ext_range_->get_range()))) {
        STORAGE_LOG(WARN, "Fail to init micro scanner, ", K(ret), K(read_handle));
      } else {
        cur_range_idx_ = read_handle.range_idx_;
        STORAGE_LOG(DEBUG,
            "success to init micro block scanner",
            K(ret),
            K(read_handle),
            K(read_handle.ext_range_->get_range()));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_block_data(micro_block_idx, block_data))) {
      STORAGE_LOG(WARN, "Fail to get block data, ", K(ret), K(micro_block_idx), K(read_handle));
    } else if (OB_FAIL(micro_scanner_->open(read_handle.get_macro_block_id(),
                   read_handle.full_meta_,
                   block_data,
                   is_left_border,
                   is_right_border))) {
      STORAGE_LOG(WARN, "Fail to open micro scanner, ", K(ret), K(read_handle), K(micro_block_idx));
    }
    if (OB_SUCC(ret)) {
      STORAGE_LOG(DEBUG,
          "Success to open micro block, ",
          K(read_handle),
          K(micro_block_idx),
          K(is_left_border),
          K(is_right_border),
          K(common::lbt()));
    }
  }
  return ret;
}

int ObSSTableRowIterator::get_skip_range_ctx(
    ObSSTableReadHandle& read_handle, const int64_t cur_micro_idx, ObSSTableSkipRangeCtx*& skip_ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(read_handle);
  UNUSED(cur_micro_idx);
  skip_ctx = NULL;
  return ret;
}

int ObSSTableRowIterator::get_gap_end(int64_t& range_idx, const common::ObStoreRowkey*& gap_key, int64_t& gap_size)
{
  int ret = OB_SUCCESS;
  range_idx = 0;
  gap_key = NULL;
  gap_size = 0;
  return ret;
}

int ObSSTableRowIterator::report_stat()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    // when hit fuse_row_cache, iter can be skipped and may not be opened yet
    STORAGE_LOG(DEBUG, "The ObSSTableRowIterator has not been opened, ", K(ret));
  } else if (OB_FAIL(ObTableStoreStatMgr::get_instance().report_stat(table_store_stat_))) {
    STORAGE_LOG(WARN, "report stat fail", K(ret), K_(table_store_stat));
  } else {
    table_store_stat_.reuse();
  }
  return ret;
}

const ObIArray<ObRowkeyObjComparer*>* ObSSTableRowIterator::get_rowkey_cmp_funcs()
{
  ObIArray<ObRowkeyObjComparer*>* cmp_funcs = nullptr;
  if (OB_NOT_NULL(sstable_) && sstable_->get_rowkey_helper().is_valid()) {
    cmp_funcs = &sstable_->get_rowkey_helper().get_compare_funcs();
  }
  return cmp_funcs;
}

int ObSSTableRowIterator::init_handle_mgr(
    const ObTableIterParam& iter_param, ObTableAccessContext& access_ctx, const void* query_range)
{
  int ret = OB_SUCCESS;
  int64_t range_count = 0;
  bool is_multi = false;
  bool is_ordered = false;
  if (OB_FAIL(get_range_count(query_range, range_count))) {
    STORAGE_LOG(WARN, "failed to get range count", K(ret), KP(query_range));
  } else if (0 >= range_count) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "range count should be greater than 0", K(ret), K(range_count));
  } else if (1 == range_count) {
    is_multi = false;
    is_ordered = false;
  } else {
    is_multi = true;
    is_ordered =
        !(((access_ctx.query_flag_.scan_order_ != ObQueryFlag::ScanOrder::Forward &&
               access_ctx.query_flag_.scan_order_ != ObQueryFlag::ScanOrder::Reverse) ||
              (access_ctx.query_flag_.is_index_back() && access_ctx.pkey_.table_id_ == iter_param.table_id_)) &&
            range_count >= USE_HANDLE_CACHE_RANGE_COUNT_THRESHOLD);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(block_handle_mgr_.init(is_multi, true, *access_ctx.allocator_))) {
      STORAGE_LOG(WARN, "failed to init block handle mgr", K(ret), K(is_multi), K(is_ordered));
    } else if (OB_FAIL(block_index_handle_mgr_.init(is_multi, is_ordered, *access_ctx.allocator_))) {
      STORAGE_LOG(WARN, "failed to init block index handle mgr", K(ret), K(is_multi), K(is_ordered));
    }
  }
  return ret;
}

int ObSSTableRowIterator::set_row_scn(const ObStoreRow*& store_row)
{
  int ret = OB_SUCCESS;
  const ObColDescIArray* out_cols = NULL;
  if (OB_FAIL(iter_param_->get_out_cols(false /*is_get*/, out_cols))) {
    STORAGE_LOG(WARN, "failed to get out cols", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < out_cols->count(); i++) {
      if (OB_HIDDEN_TRANS_VERSION_COLUMN_ID == out_cols->at(i).col_id_) {
        if (store_row->row_val_.cells_[i].is_nop_value()) {
          store_row->row_val_.cells_[i].set_int(sstable_->get_snapshot_version());
        } else {
          int64_t version = -store_row->row_val_.cells_[i].get_int();
          if (version > 0) {
            store_row->row_val_.cells_[i].set_int(version);
          } else {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "scn should be greater than 0", K(ret), K(version), KP(store_row));
          }
        }
      }
    }
  }
  return ret;
}

int ObSSTableRowIterator::check_row_locked(ObSSTableReadHandle& read_handle, ObStoreRowLockState& lock_state)
{
  int ret = OB_SUCCESS;
  lock_state.is_locked_ = false;

  switch (read_handle.state_) {
    case ObSSTableRowState::NOT_EXIST:
      break;
    case ObSSTableRowState::IN_ROW_CACHE:
      break;
    case ObSSTableRowState::IN_BLOCK:
      ret = check_block_row_lock(read_handle, lock_state);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid row state", K(ret), K(read_handle.state_));
  }

  if (OB_SUCC(ret)) {
    ++cur_read_handle_pos_;
    STORAGE_LOG(
        DEBUG, "get exist row", K(read_handle.state_), K(read_handle.ext_rowkey_->get_store_rowkey()), KP(this));
  }

  return ret;
}

int ObSSTableRowIterator::check_block_row_lock(ObSSTableReadHandle& read_handle, ObStoreRowLockState& lock_state)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData block_data;

  if (NULL == micro_lock_checker_) {
    if (NULL == (micro_lock_checker_ = OB_NEWx(ObMicroBlockRowLockChecker, access_ctx_->allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Fail to allocate micro lock checker, ", K(ret));
    } else if (OB_FAIL(micro_lock_checker_->init(*iter_param_, *access_ctx_, sstable_))) {
      STORAGE_LOG(WARN, "Fail to init micro lock checker, ", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    for (int64_t i = read_handle.micro_begin_idx_; OB_SUCC(ret) && i <= read_handle.micro_end_idx_; ++i) {
      if (OB_FAIL(get_block_data(i, block_data))) {
        STORAGE_LOG(WARN, "Fail to get block data, ", K(ret), K(read_handle));
      } else {
        ret = micro_lock_checker_->check_row_locked(*access_ctx_->store_ctx_->trans_table_guard_,
            access_ctx_->store_ctx_->trans_id_,
            read_handle.ext_rowkey_->get_store_rowkey(),
            read_handle.full_meta_,
            block_data,
            read_handle.rowkey_helper_,
            lock_state);
      }
    }
  }

  return ret;
}

}  // namespace storage
}  // namespace oceanbase
