/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE_COMPACTION

#include "ob_merge_vector_store.h"
#include "storage/compaction/ob_basic_tablet_merge_ctx.h"
#include "storage/blocksstable/ob_micro_block_row_scanner.h"

namespace oceanbase
{
namespace compaction
{
/* ObMergeVectorStoreLayoutParam */
ObMergeVectorStoreLayoutParam::ObMergeVectorStoreLayoutParam()
  : column_count_(0),
    rowkey_column_cnt_(0),
    datum_utils_(nullptr),
    static_param_(nullptr)
{
}

void ObMergeVectorStoreLayoutParam::destroy()
{
  column_count_ = 0;
  rowkey_column_cnt_ = 0;
  datum_utils_ = nullptr;
  static_param_ = nullptr;
}

bool ObMergeVectorStoreLayoutParam::is_valid() const
{
  return column_count_ > 0 &&
         rowkey_column_cnt_ >= 0 &&
         column_count_ >= rowkey_column_cnt_ &&
         (nullptr == datum_utils_ || datum_utils_->is_valid()) &&
         (nullptr != static_param_ && static_param_->compaction_batch_size_ > 1);
}

int64_t ObMergeVectorStoreLayoutParam::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    J_KV(K_(column_count), K_(rowkey_column_cnt), KP_(datum_utils));
    if (OB_NOT_NULL(static_param_)) {
      J_COMMA();
      J_KV("merge_snapshot_version", static_param_->dag_param_.merge_version_);
      J_COMMA();
      J_KV("batch_capacity", static_param_->compaction_batch_size_);
    }
    J_OBJ_END();
  }
  return pos;
}

int64_t ObMergeVectorStoreLayoutParam::get_compaction_batch_size() const
{
  int64_t compaction_batch_size = 1;
  if (nullptr != static_param_) {
    compaction_batch_size = static_param_->compaction_batch_size_;
  }
  return compaction_batch_size;
}

int64_t ObMergeVectorStoreLayoutParam::get_merge_snapshot_version() const
{
  int64_t merge_snapshot_version = 0;
  if (nullptr != static_param_) {
    merge_snapshot_version = static_param_->dag_param_.merge_version_;
  }
  return merge_snapshot_version;
}

/* ObMergeVectorStorePair */
int ObMergeVectorStorePair::init(
    const ObMergeVectorStoreLayoutParam &layout_param,
    const common::ObIArray<share::schema::ObColDesc> &col_descs,
    const blocksstable::ObDatumRow *default_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(read_store_.init(layout_param,
                               col_descs,
                               false/*is_continuous*/,
                               default_row))) {
    LOG_WARN("failed to init read store", K(ret));
  } else if (OB_FAIL(write_store_.init(layout_param,
                                       col_descs,
                                       true/*is_continuous*/,
                                       default_row))) {
    LOG_WARN("failed to init write store", K(ret));
  }
  return ret;
}

void ObMergeVectorStorePair::reset()
{
  read_store_.reset();
  write_store_.reset();
}

ObMergeVectorStore::ObMergeVectorStore()
  : is_inited_(false),
    need_force_flush_(false),
    single_row_(nullptr),
    layout_param_(nullptr),
    row_count_(0),
    total_mem_usage_(0),
    mem_limit_(DEFAULT_BATCH_BUFFER_LIMIT),
    incremental_row_count_(0),
    last_row_(),
    vectors_(),
    allocator_("MergeVec"),
    vector_allocator_("MergeVecVec"),
    cell_data_ptrs_(nullptr),
    len_array_(nullptr),
    row_ids_(nullptr),
    row_buf_(),
    default_datums_()
{
}

ObMergeVectorStore::~ObMergeVectorStore()
{
  reset();
}

int ObMergeVectorStore::init(
    const ObMergeVectorStoreLayoutParam &layout_param,
    const common::ObIArray<share::schema::ObColDesc> &col_descs,
    const bool is_continuous,
    const blocksstable::ObDatumRow *default_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMergeVectorStore has been initialized", K(ret));
  } else if (OB_UNLIKELY(!layout_param.is_valid() || layout_param.column_count_ != col_descs.count() ||
                  (nullptr != default_row && default_row->count_ != layout_param.column_count_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(ret), K(layout_param), K(col_descs), K(is_continuous), KPC(default_row));
  } else {
    layout_param_ = &layout_param;
    row_count_ = 0;
    const int64_t batch_capacity = layout_param_->get_compaction_batch_size();
    if (OB_FAIL(init_default_datums(default_row))) {
      LOG_WARN("Failed to init default datums", K(ret));
    } else if (OB_FAIL(init_vectors(col_descs, is_continuous))) {
      LOG_WARN("Failed to init vectors", K(ret), K(col_descs));
    } else {
      // Allocate temporary buffers for batch scan.
      // row_ids_ is placed first (int32_t, 4-byte aligned);
      const int64_t row_ids_size = sizeof(int32_t) * batch_capacity;
      const int64_t cell_data_ptrs_size = sizeof(char *) * batch_capacity;
      const int64_t len_array_size = sizeof(uint32_t) * batch_capacity;
      const int64_t total_buf_size = row_ids_size + cell_data_ptrs_size + len_array_size;
      void *ptr = nullptr;
      if (OB_ISNULL(ptr = allocator_.alloc(total_buf_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to allocate memory", K(ret), K(total_buf_size));
      } else {
        MEMSET(ptr, 0, total_buf_size);
        row_ids_ = static_cast<int32_t*>(ptr);
        ptr = (void*)(static_cast<char*>(ptr) + row_ids_size);
        cell_data_ptrs_ = reinterpret_cast<const char**>(ptr);
        ptr = (void*)(static_cast<char*>(ptr) + cell_data_ptrs_size);
        len_array_ = static_cast<uint32_t*>(ptr);
        if (OB_FAIL(row_buf_.init(allocator_, layout_param_->column_count_))) {
          LOG_WARN("Failed to init row_buf", K(ret), KPC(layout_param_));
        } else if (layout_param_->datum_utils_ != nullptr) {
          if (OB_FAIL(last_row_.init(allocator_, layout_param_->rowkey_column_cnt_))) {
            LOG_WARN("Failed to init last row", K(ret), KPC(layout_param_));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
    LOG_DEBUG("ObMergeVectorStore initialized", KPC(this), K(layout_param));
  }
  return ret;
}

int ObMergeVectorStore::init_default_datums(const blocksstable::ObDatumRow *default_row)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(default_row)) {
    default_datums_.set_allocator(&allocator_);
    const int64_t column_count = get_column_count();
    if (default_row->count_ != column_count) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Default row column count mismatch", K(ret), K(default_row->count_), K(column_count));
    } else if (OB_FAIL(default_datums_.init(default_row->count_))) {
      LOG_WARN("Failed to init default datums", K(ret), K(default_row->count_));
    } else {
      // shallow copy
      for (int64_t i = 0; OB_SUCC(ret) && i < default_row->count_; i++) {
        if (OB_FAIL(default_datums_.push_back(default_row->storage_datums_[i]))) {
          LOG_WARN("Failed to push back default datum", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int ObMergeVectorStore::init_vectors(const common::ObIArray<share::schema::ObColDesc> &col_descs, const bool is_continuous)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(vectors_.prepare_allocate(col_descs.count()))) {
    LOG_WARN("Failed to prepare allocate", K(ret), K(col_descs.count()));
  } else {
    const int64_t batch_capacity = get_batch_capacity();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_descs.count(); i++) {
      const share::schema::ObColDesc &col_desc = col_descs.at(i);
      ObCompactionVector *vector = nullptr;
      if (OB_FAIL(ObCompactionVector::create_vector(
          is_continuous, col_desc, batch_capacity, vector_allocator_, vector))) {
        LOG_WARN("Failed to create vector", K(ret), K(col_desc), K(batch_capacity));
      } else {
        vectors_.at(i) = vector;
      }
    }
    if (OB_SUCC(ret)) {
      total_mem_usage_ = vector_allocator_.total();
    }
  }
  return ret;
}

void ObMergeVectorStore::reset()
{
  is_inited_ = false;
  need_force_flush_ = false;
  single_row_ = nullptr;
  for (int64_t i = 0; i < vectors_.count(); ++i) {
    if (OB_NOT_NULL(vectors_.at(i))) {
      vectors_.at(i)->~ObCompactionVector();
      vectors_.at(i) = nullptr;
    }
  }
  vectors_.reset();
  row_buf_.reset();
  default_datums_.reset();
  last_row_.reset();
  layout_param_ = nullptr;
  row_count_ = 0;
  total_mem_usage_ = 0;
  mem_limit_ = DEFAULT_BATCH_BUFFER_LIMIT;
  incremental_row_count_ = 0;
  allocator_.reset();
  vector_allocator_.reset();
  row_ids_ = nullptr;
  cell_data_ptrs_ = nullptr;
  len_array_ = nullptr;
}

void ObMergeVectorStore::reuse()
{
  // Simply reset row count; vectors will be overwritten on next append
  total_mem_usage_ = 0;
  incremental_row_count_ = 0;
  need_force_flush_ = false;
  single_row_ = nullptr;
  const int64_t batch_capacity = get_batch_capacity();
  if (0 < row_count_) {
    for (int64_t i = 0; i < vectors_.count(); i++) {
      if (OB_NOT_NULL(vectors_[i])) {
        vectors_[i]->reuse(batch_capacity);
      } else {
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "Unexpected null vector", K(i));
      }
    }
  }
  row_count_ = 0;
}

int ObMergeVectorStore::set_single_row(const blocksstable::ObDatumRow *row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("single row is null", K(ret));
  } else if (OB_UNLIKELY(get_row_count() != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("single row already set", K(ret));
  } else if (row->row_flag_.is_delete()) { // skip delete row
  } else {
    single_row_ = row;
  }
  return ret;
}

int ObMergeVectorStore::fill_last_rowkey(const int64_t append_rows)
{
  int ret = OB_SUCCESS;
  if (nullptr == layout_param_->datum_utils_) {
    // do nothing // is cg
  } else {
    const int64_t last_row_idx = row_count_ - append_rows - 1;
    const int64_t cur_row_idx = row_count_ - 1;
    const int64_t schema_rowkey_cnt = layout_param_->rowkey_column_cnt_ - storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    blocksstable::ObDatumRowkey last_rowkey;
    // Only shallow copy is needed, as deep copy data already exists in vector.
    // Hold previous row's schema rowkey in last_row_ so row_buf_ can be filled with the
    // current row without aliasing last_rowkey.datums_ (ObDatumRowkey::assign stores pointers only).
    if (last_row_idx >= 0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < schema_rowkey_cnt; i++) {
        if (OB_FAIL(vectors_.at(i)->get_datum(last_row_idx, last_row_.storage_datums_[i]))) {
          LOG_WARN("Failed to get datum from vector", K(ret), K(i));
        }
      }
      if (FAILEDx(last_rowkey.assign(last_row_.storage_datums_, schema_rowkey_cnt))) {
        LOG_WARN("Failed to assign last rowkey", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < layout_param_->rowkey_column_cnt_; i++) {
      if (OB_FAIL(vectors_.at(i)->get_datum(cur_row_idx, row_buf_.storage_datums_[i]))) {
        LOG_WARN("Failed to get datum from vector", K(ret), K(i));
      }
    }
    if (FAILEDx(check_order(row_buf_, last_rowkey))) {
      LOG_WARN("Failed to check order", K(ret));
    }
  }
  return ret;
}

// only for major merge now
int ObMergeVectorStore::check_order(const blocksstable::ObDatumRow &cur_row, const blocksstable::ObDatumRowkey &last_rowkey)
{
  int ret = OB_SUCCESS;
  ObDatumRowkey cur_key;
  int32_t compare_result = 0;
  const int64_t rowkey_column_cnt = layout_param_->rowkey_column_cnt_;
  const int64_t schema_rowkey_cnt = rowkey_column_cnt - storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  const int64_t trans_version_col_idx = ObMultiVersionRowkeyHelpper::get_trans_version_col_store_index(schema_rowkey_cnt, true/*is_multi_version*/);
  const int64_t sql_sequence_col_idx = ObMultiVersionRowkeyHelpper::get_sql_sequence_col_store_index(schema_rowkey_cnt, true/*is_multi_version*/);
  int64_t cur_row_version = cur_row.storage_datums_[trans_version_col_idx].get_int();
  int64_t cur_row_sql_sequence = cur_row.storage_datums_[sql_sequence_col_idx].get_int();
  const int64_t merge_snapshot_version = layout_param_->get_merge_snapshot_version();
  const blocksstable::ObStorageDatumUtils *datum_utils = layout_param_->datum_utils_;

  if (cur_row_version >= 0) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "invalid trans_version or sql_sequence", K(ret), K(cur_row));
  } else if (-cur_row_version > merge_snapshot_version) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "invalid trans_version", K(ret), K(cur_row), K(merge_snapshot_version));
  } else if (last_rowkey.is_valid()) {
    if (OB_FAIL(cur_key.assign(cur_row.storage_datums_, schema_rowkey_cnt))) {
      STORAGE_LOG(WARN, "Failed to assign cur key", K(ret));
    } else if (OB_FAIL(cur_key.compare(last_rowkey, *datum_utils, compare_result))) {
      STORAGE_LOG(WARN, "Failed to compare last key", K(ret), K(cur_key), K(last_rowkey));
    } else if (OB_UNLIKELY(compare_result <= 0)) {
      ret = OB_ROWKEY_ORDER_ERROR;
      STORAGE_LOG(ERROR, "input rowkey is less then last rowkey.", K(cur_key), K(last_rowkey), K(ret));
    }
  }
  return ret;
}

int ObMergeVectorStore::append_row(
    const blocksstable::ObDatumRow &row,
    const bool need_check_order,
    const common::ObIArrayWrap<uint16_t> *cols,
    const bool is_incremental)
{
  int ret = OB_SUCCESS;
  const int64_t column_count = get_column_count();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMergeVectorStore not initialized", K(ret));
  } else if (OB_UNLIKELY(row_count_ >= get_batch_capacity() || has_single_row())) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (OB_UNLIKELY(nullptr != cols && cols->count() != column_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Column count mismatch", K(ret), K(cols->count()), K(column_count));
  } else if (OB_UNLIKELY(nullptr == cols && row.count_ != column_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Column count mismatch", K(ret), K(column_count), K(row.count_));
  } else if (row.row_flag_.is_delete()) { // skip delete row
  } else {
    total_mem_usage_ = vector_allocator_.total();
    // Append each datum to corresponding vector
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count; i++) {
      const int64_t src_col_idx = cols == nullptr ? i : cols->at(i);
      const blocksstable::ObStorageDatum &src_datum = row.storage_datums_[src_col_idx];
      if (OB_FAIL(append_datum_to_vector(i, datum_or_default(src_datum, i)))) {
        if (OB_BUF_NOT_ENOUGH != ret) {
          LOG_WARN("Failed to append datum to vector", K(ret), K(i));
        }
      }
    }

    if (OB_SUCC(ret)) {
      row_count_++;
      if (is_incremental) {
        incremental_row_count_++;
      }
      if (need_check_order && OB_FAIL(fill_last_rowkey())) {
        LOG_WARN("Failed to fill last rowkey", K(ret));
      } else {
        LOG_TRACE("append_row success", K(row), K(row_count_), K(row.count_), K(this));
      }
    }
  }

  return ret;
}

int ObMergeVectorStore::is_row_filtered(const int64_t idx, ObCompactionFilterHandle *filter_handle, bool &is_filtered)
{
  int ret = OB_SUCCESS;
  is_filtered = false;
  if (OB_NOT_NULL(filter_handle) && filter_handle->is_valid()) {
    ObICompactionFilter::ObFilterRet filter_ret = ObICompactionFilter::FILTER_RET_MAX;
    if (OB_FAIL(get_datum_row(idx, row_buf_))) {
      LOG_WARN("failed to get datum row for filter", K(ret), K(idx));
    } else if (OB_FAIL(filter_handle->filter(row_buf_, filter_ret))) {
      LOG_WARN("failed to filter row", K(ret), K(idx));
    } else if (ObICompactionFilter::FILTER_RET_REMOVE == filter_ret) {
      is_filtered = true;
    }
  }
  return ret;
}

// used to append batch to write buffer from read buffer
int ObMergeVectorStore::append_batch(
    const ObMergeVectorStore &other,
    int64_t &batch_idx,
    const common::ObIArrayWrap<uint16_t> *cols,
    ObCompactionFilterHandle *filter_handle,
    const bool is_incremental)
{
  int ret = OB_SUCCESS;
  const int64_t column_count = get_column_count();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMergeVectorStore not initialized", K(ret));
  } else if (OB_UNLIKELY(nullptr != cols && cols->count() != column_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Column count mismatch", K(ret), K(cols->count()), K(column_count));
  } else if (OB_UNLIKELY(nullptr == cols && column_count != other.get_column_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Column count mismatch", K(ret), K(column_count), K(other.get_column_count()));
  } else {
    int64_t append_rows = 0;
    bool is_filtered = false;
    while (OB_SUCC(ret) && batch_idx < other.get_row_count()) {
      if (OB_FAIL(const_cast<ObMergeVectorStore &>(other).is_row_filtered(batch_idx, filter_handle, is_filtered))) {
        LOG_WARN("failed to check if row is filtered", K(ret), K(batch_idx));
      } else if (is_filtered) { // skip rows filtered by compaction filter
        ++batch_idx;
      } else if (OB_UNLIKELY(row_count_ >= get_batch_capacity())) {
        ret = OB_BUF_NOT_ENOUGH;
      } else if (OB_FAIL(other.get_datum_row(batch_idx, row_buf_, cols))) {
        LOG_WARN("Failed to get datum row", K(ret), K(batch_idx));
      } else if (OB_FAIL(append_row(row_buf_, false/*need_check_order*/, nullptr, is_incremental))) {
        if (OB_BUF_NOT_ENOUGH != ret) {
          LOG_WARN("Failed to append row", K(ret), K(batch_idx));
        }
      } else {
        batch_idx++;
        append_rows++;
      }
    }
    if (OB_SUCC(ret) || OB_BUF_NOT_ENOUGH == ret) {
      int tmp_ret = OB_SUCCESS;
      if (append_rows > 0 && OB_TMP_FAIL(fill_last_rowkey(append_rows))) {
        LOG_WARN("Failed to fill last rowkey", K(tmp_ret), K(ret));
        ret = tmp_ret; // failed // overwrite ret
      }
    }
  }
  return ret;
}

int ObMergeVectorStore::get_batch_datum_rows(blocksstable::ObBatchDatumRows &batch_rows)
{
  int ret = OB_SUCCESS;
  const int64_t column_count = get_column_count();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMergeVectorStore not initialized", K(ret));
  } else if (OB_UNLIKELY(is_empty())) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("Vector store is empty", K(ret));
  } else {
    // Clear existing batch_rows
    batch_rows.reset();
    batch_rows.row_flag_.set_flag(blocksstable::ObDmlFlag::DF_INSERT);
    // Set row count
    batch_rows.row_count_ = row_count_;

    // Convert vectors to ObBatchDatumRows format
    if (OB_FAIL(batch_rows.vectors_.reserve(column_count))) {
      LOG_WARN("Failed to reserve vectors", K(ret), K(column_count));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < column_count; i++) {
        if (OB_ISNULL(vectors_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected null vector", K(ret), K(i));
        } else if (OB_FAIL(batch_rows.vectors_.push_back(vectors_.at(i)->get_vector()))) {
          LOG_WARN("Failed to push back vector", K(ret), K(i));
        }
      }
    }
  }

  return ret;
}

int ObMergeVectorStore::get_datum_row(
    const int64_t row_idx,
    blocksstable::ObDatumRow &datum_row,
    const common::ObIArrayWrap<uint16_t> *cols) const
{
  int ret = OB_SUCCESS;
  const int64_t column_count = get_column_count();
  datum_row.reuse();
  if (OB_UNLIKELY(row_idx < 0 || row_idx >= get_row_count() ||
                  !datum_row.is_valid() ||
                  (nullptr != cols && cols->count() != datum_row.get_column_count()) ||
                  (nullptr == cols && datum_row.get_column_count() != column_count))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(ret), K(row_idx), K_(row_count), K(datum_row));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_row.get_column_count(); i++) {
      const int64_t col_idx = cols == nullptr ? i : cols->at(i);
      if (has_single_row()) { // get from single_row_
        datum_row.storage_datums_[i] = single_row_->storage_datums_[col_idx];
      } else if (OB_FAIL(vectors_.at(col_idx)->get_datum(row_idx, datum_row.storage_datums_[i]))) {
        LOG_WARN("Failed to get datum from vector", K(ret), K(i), K(row_idx));
      }
    }
  }
  return ret;
}

int ObMergeVectorStore::append_datum_to_vector(
    const int64_t col_idx,
    const blocksstable::ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  const int64_t column_count = get_column_count();
  if (OB_UNLIKELY(col_idx < 0 || col_idx >= column_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid column index", K(ret), K(col_idx), K(column_count));
  } else if (OB_FAIL(vectors_.at(col_idx)->append_datum(row_count_, datum))) {
    LOG_WARN("Failed to append datum to vector", K(ret), K(col_idx), K(row_count_));
  } else if (OB_UNLIKELY(row_count_ > 0 &&
      (total_mem_usage_ += vectors_.at(col_idx)->get_extra_mem_usage()) > mem_limit_)) { // ensure to save one row at least
    ret = OB_BUF_NOT_ENOUGH;
  }
  return ret;
}

// for batch scan
// only limited by batch_capacity_
int ObMergeVectorStore::fill_rows(
    const common::ObIArrayWrap<uint16_t> *cols,
    blocksstable::ObIMicroBlockRowScanner &scanner,
    int64_t &begin_index,
    const int64_t end_index)
{
  int ret = OB_SUCCESS;
  blocksstable::ObIMicroBlockReader *reader = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMergeVectorStore not initialized", K(ret));
  } else if (OB_ISNULL(reader = scanner.get_reader())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null reader", K(ret));
  } else if (OB_FAIL(fill_rows_from_reader(cols, *reader, begin_index, end_index))) {
    LOG_WARN("Failed to fill rows from scanner", K(ret), K(begin_index), K(end_index));
  }
  return ret;
}

int ObMergeVectorStore::fill_rows_from_reader(
    const common::ObIArrayWrap<uint16_t> *cols,
    blocksstable::ObIMicroBlockReader &reader,
    int64_t &begin_index,
    const int64_t end_index)
{
  int ret = OB_SUCCESS;
  int64_t row_cap = 0;
  const int64_t column_count = get_column_count();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMergeVectorStore not initialized", K(ret));
  } else if (nullptr != cols && OB_UNLIKELY(cols->count() != column_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Column projection count mismatch", K(ret), K(column_count), K(cols->count()));
  } else if (OB_FAIL(get_batch_row_count(begin_index, end_index, row_cap))) {
    LOG_WARN("Failed to get batch row count", K(ret), K(begin_index), K(end_index));
  } else if (row_cap <= 0) {
  } else if (OB_FAIL(inner_fill_rows_from_reader(cols, &reader, begin_index, row_cap))) {
    LOG_WARN("Failed to fill rows from reader", K(ret), K(row_cap));
  } else {
    begin_index += row_cap;
    row_count_ += row_cap;
    LOG_DEBUG("ObMergeVectorStore filled rows", K(row_cap), K(row_count_), K(begin_index), K(end_index));
  }
  return ret;
}

// TODO: used for get_rows
int ObMergeVectorStore::get_row_ids(const int64_t begin_index, const int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(begin_index < 0 || row_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid index range", K(ret), K(begin_index), K(row_count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
      row_ids_[i] = begin_index + i;
    }
  }
  return ret;
}

int ObMergeVectorStore::get_batch_row_count(
    int64_t begin_index,
    const int64_t end_index,
    int64_t &row_count)
{
  int ret = OB_SUCCESS;
  row_count = 0;

  if (OB_UNLIKELY(begin_index < 0 || end_index < begin_index)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid index range", K(ret), K(begin_index), K(end_index));
  } else if (has_single_row()) {
    row_count = 0;
  } else {
    // Calculate how many rows we can read
    const int64_t remain_rows = end_index - begin_index;
    const int64_t remain_capacity = get_batch_capacity() - row_count_;
    row_count = MIN(remain_rows, remain_capacity);
  }

  return ret;
}

int ObMergeVectorStore::inner_fill_rows_from_reader(
    const common::ObIArrayWrap<uint16_t> *cols,
    blocksstable::ObIMicroBlockReader *reader,
    const int64_t begin_index,
    const int64_t row_cap)
{
  int ret = OB_SUCCESS;
  const int64_t column_count = get_column_count();

  if (OB_ISNULL(reader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KP(reader));
  } else if (nullptr != cols && OB_UNLIKELY(cols->count() != column_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Column projection count mismatch", K(ret), K(column_count), K(cols->count()));
  } else {
    switch (reader->get_type())
    {
      case blocksstable::ObIMicroBlockReader::Reader:
      case blocksstable::ObIMicroBlockReader::NewFlatReader: {
        // For flat reader: read rows one by one
        // The flat reader doesn't have optimized batch get_rows for vectors yet
        int64_t row_idx = begin_index;
        for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; i++, row_idx++) {
          if (OB_FAIL(reader->get_row(row_idx, row_buf_))) {
            LOG_WARN("Failed to get row", K(ret), K(i), K(row_idx));
          } else if (row_buf_.row_flag_.is_delete()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected delete row", K(ret), K(row_idx), K(row_buf_));
          } else {
            for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < column_count; col_idx++) {
              const int64_t src_col_idx = nullptr == cols ? col_idx : cols->at(col_idx);
              const blocksstable::ObStorageDatum &src_datum = row_buf_.storage_datums_[src_col_idx];
              if (OB_FAIL(vectors_.at(col_idx)->append_datum(row_count_ + i, datum_or_default(src_datum, col_idx)))) {
                LOG_WARN("failed to append datum to vector", K(ret), K(row_count_), K(i));
              }
            }
          }
        }
        break;
      }
      case blocksstable::ObIMicroBlockReader::Decoder:
      case blocksstable::ObIMicroBlockReader::CSDecoder: {
        if (OB_FAIL(get_row_ids(begin_index, row_cap))) {
          LOG_WARN("Failed to get row ids", K(ret), K(begin_index), K(row_cap));
        } else if (OB_FAIL(reader->get_rows(
            cols, get_default_datums(), row_ids_, row_cap, row_count_, cell_data_ptrs_, len_array_, vectors_))) {
          LOG_WARN("Failed to get rows", K(ret), K(row_cap));
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("Unsupported reader type", K(ret), K(reader->get_type()));
        break;
      }
    }
  }

  return ret;
}

} // namespace compaction
} // namespace oceanbase
