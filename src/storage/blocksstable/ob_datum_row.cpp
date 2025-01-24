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

#define USING_LOG_PREFIX STORAGE
#include "ob_datum_row.h"
#include "storage/ob_i_store.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"

namespace oceanbase
{
using namespace common;
namespace blocksstable
{

const char *get_dml_str(ObDmlFlag dml_flag)
{
  STATIC_ASSERT(static_cast<int64_t>(DF_MAX) == ARRAYSIZEOF(ObDmlFlagStr), "dml str len is mismatch");
  const char *ret_str = nullptr;
  if (dml_flag >= DF_MAX) {
    ret_str = "invalid_dml";
  } else {
    ret_str = ObDmlFlagStr[dml_flag];
  }
  return ret_str;
}

OB_SERIALIZE_MEMBER(ObDmlRowFlag, whole_flag_);

OB_SERIALIZE_MEMBER(ObMultiVersionRowFlag, flag_);

/*
 *ObConstDatumRow
 */
int ObConstDatumRow::set_datums_ptr(char *datums_ptr)
{
  int ret = OB_SUCCESS;
  if (datums_ != reinterpret_cast<ObDatum *>(datums_ptr + datum_row_offset_)) {
    char *ptr = reinterpret_cast<char *>(datums_);
    datums_ = reinterpret_cast<ObDatum *>(datums_ptr + datum_row_offset_);
    for (int64_t i = 0; OB_SUCC(ret) && i < count_; i++) {
      if (OB_UNLIKELY(datums_[i].ptr_ <= ptr)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected datum ptr", K(ret), K(ptr), K(i), K(datums_[i]));
      } else {
        datums_[i].ptr_ = datums_[i].ptr_ - ptr + reinterpret_cast<char *>(datums_);
      }
    }
  }
  return ret;
}

/*
 *ObDatumRow
 */

ObDatumRow::ObDatumRow(const uint64_t tenant_id)
  : local_allocator_("ObDatumRow", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id, ObCtxIds::DEFAULT_CTX_ID),
    count_(0),
    fast_filter_skipped_(false),
    have_uncommited_row_(false),
    row_flag_(),
    mvcc_row_flag_(),
    trans_id_(),
    scan_index_(0),
    group_idx_(0),
    snapshot_version_(0),
    storage_datums_(nullptr),
    datum_buffer_(),
    trans_info_(nullptr)
{
  if (share::is_reserve_mode()) {
    local_allocator_.set_ctx_id(ObCtxIds::MERGE_RESERVE_CTX_ID);
  }
}

ObDatumRow::~ObDatumRow()
{}

int ObDatumRow::init(ObIAllocator &allocator, const int64_t capacity, char *trans_info_ptr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_valid())) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObDatumRow init twice", K(ret), K(*this));
  } else if (OB_FAIL(datum_buffer_.init(allocator))) {
    STORAGE_LOG(WARN, "Failed to init datum buffer", K(ret));
  } else if (OB_FAIL(datum_buffer_.reserve(capacity))) {
    STORAGE_LOG(WARN, "Failed to reserve datum buffer", K(ret), K(capacity));
  } else {
    storage_datums_ = datum_buffer_.get_datums();
    count_ = capacity;
    // trans_info_ptr maybe is nullptr,
    // ObDatumRow does not care about the free of trans_info_ptr's memory
    trans_info_ = trans_info_ptr;

    STORAGE_LOG(DEBUG, "succeed to init datum row", K(ret), K_(count));
  }

  return ret;

}

int ObDatumRow::init(const int64_t capacity)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_valid())) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObDatumRow init twice", K(ret), K(*this));
  } else if (OB_UNLIKELY(capacity <= 0 || capacity > 2 * OB_USER_ROW_MAX_COLUMNS_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init datumrow", K(ret), K(capacity));
  } else {
    ret = init(local_allocator_, capacity);
  }

  return ret;
}

//// only for transformer which never use old_row_
//int ObDatumRow::init_buf(char *buf, const int64_t buf_size, const int64_t capacity)
//{
  //int ret = OB_SUCCESS;

  //if (OB_UNLIKELY(is_valid())) {
    //ret = OB_INIT_TWICE;
    //STORAGE_LOG(WARN, "ObDatumRow init twice", K(ret), K(*this));
  //} else if (OB_UNLIKELY(nullptr == buf || capacity <= 0 || buf_size < capacity * sizeof(ObStorageDatum))) {
    //ret = OB_INVALID_ARGUMENT;
    //STORAGE_LOG(WARN, "Invalid argument to init datum row", K(ret), KP(buf), K(buf_size), K(capacity));
  //} else {
    //storage_datums_ = new (buf) ObStorageDatum [capacity];
    //count_ = capacity_ = capacity;
    //allocator_ = nullptr;
    //old_row_.reset();
  //}

  //return ret;
//}

int ObDatumRow::reserve(const int64_t capacity, const bool keep_data)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDatumRow is not inited", K(ret), K(*this));
  } else if (OB_UNLIKELY(capacity <= 0 || capacity > 2 * OB_USER_ROW_MAX_COLUMNS_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to reserve datum row", K(ret), K(capacity));
  } else if (capacity <= get_capacity()) {
    // skip
  } else if (OB_FAIL(datum_buffer_.reserve(capacity, keep_data))) {
    STORAGE_LOG(WARN, "Failed to reserve datum buffer", K(ret), K(capacity));
  } else {
    storage_datums_ = datum_buffer_.get_datums();
  }
  if (OB_SUCC(ret)) {
    mvcc_row_flag_.reset();
    trans_id_.reset();
    scan_index_ = 0;
    group_idx_ = 0;
    snapshot_version_ = 0;
    fast_filter_skipped_ = false;
    have_uncommited_row_ = false;
  }

  return ret;
}


void ObDatumRow::reset()
{
  fast_filter_skipped_ = false;
  datum_buffer_.reset();
  storage_datums_ = nullptr;
  snapshot_version_ = 0;
  group_idx_ = 0;
  scan_index_ = 0;
  trans_id_.reset();
  mvcc_row_flag_.reset();
  row_flag_.reset();
  count_ = 0;
  local_allocator_.reset();
  trans_info_ = nullptr;
}

void ObDatumRow::reuse()
{
  if (nullptr != storage_datums_) {
    for (int64_t i = 0; i < count_; i++) {
      storage_datums_[i].reuse();
    }
  }
  mvcc_row_flag_.reset();
  trans_id_.reset();
  scan_index_ = 0;
  group_idx_ = 0;
  snapshot_version_ = 0;
  fast_filter_skipped_ = false;
  have_uncommited_row_ = false;
  if (OB_NOT_NULL(trans_info_)) {
    trans_info_[0] = '\0';
  }
}

int ObDatumRow::deep_copy(const ObDatumRow &src, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!src.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to deep copy datum row", K(ret), K(src));
  } else if (OB_UNLIKELY(get_capacity() < src.count_ || nullptr == storage_datums_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected local datum row to deep copy", K(ret), KPC(this));
  } else if (OB_FAIL(copy_attributes_except_datums(src))) {
    STORAGE_LOG(WARN, "copy attribute from other failed", K(ret), K(src));
  } else {
    for(int64_t i = 0; OB_SUCC(ret) && i < count_; i++) {
      if (OB_FAIL(storage_datums_[i].deep_copy(src.storage_datums_[i], allocator))) {
        STORAGE_LOG(WARN, "Failed to deep copy storage datum", K(ret), K(src.storage_datums_[i]));
      }
    }
  }

  return ret;
}

int ObDatumRow::from_store_row(const storage::ObStoreRow &store_row)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    STORAGE_LOG(WARN, "ObDatumRow is not inited", K(ret), K(*this));
  } else if (OB_FAIL(reserve(store_row.row_val_.count_))) {
    STORAGE_LOG(WARN, "Failed to reserve datums", K(ret), K(store_row));
  } else {
    count_ = store_row.row_val_.count_;
    row_flag_ = store_row.flag_;
    mvcc_row_flag_ = store_row.row_type_flag_;
    trans_id_ = store_row.trans_id_;
    scan_index_ = store_row.scan_index_;
    group_idx_ = store_row.group_idx_;
    snapshot_version_ = store_row.snapshot_version_;
    fast_filter_skipped_ = store_row.fast_filter_skipped_;
    have_uncommited_row_ = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < count_; i++) {
      if (OB_FAIL(storage_datums_[i].from_obj_enhance(store_row.row_val_.cells_[i]))) {
        STORAGE_LOG(WARN, "Failed to transfer obj to datum", K(ret), K(i), K(store_row.row_val_.cells_[i]));
      }
    }
  }

  return ret;
}

int ObDatumRow::is_datums_changed(const ObDatumRow &other, bool &is_changed) const
{
  int ret = OB_SUCCESS;
  is_changed = false;
  if (OB_UNLIKELY(count_ == 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected count", K(ret));
  } else if (count_ != other.count_) {
    is_changed = true;
  } else {
    int cmp_ret = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
      if (storage_datums_[i].is_nop() || storage_datums_[i] == other.storage_datums_[i]) {
      } else {
        is_changed = true;
        break;
      }
    }
  }
  return ret;
}

int ObDatumRow::copy_attributes_except_datums(const ObDatumRow &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to deep copy datum row", K(ret), K(other));
  } else if (OB_UNLIKELY(get_capacity() < other.count_ || nullptr == storage_datums_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected local datum row to deep copy", K(ret), KPC(this));
  } else {
    count_ = other.count_;
    row_flag_ = other.row_flag_;
    mvcc_row_flag_  = other.mvcc_row_flag_;
    trans_id_ = other.trans_id_;
    scan_index_ = other.scan_index_;
    group_idx_ = other.group_idx_;
    snapshot_version_ = other.snapshot_version_;
    fast_filter_skipped_ = other.fast_filter_skipped_;
    have_uncommited_row_ = other.have_uncommited_row_;
  }
  return ret;
}

int ObDatumRow::shallow_copy(const ObDatumRow &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected error for shallow copy invalid datum row", K(ret), K(*this), K(other));
  } else {
    trans_info_ = nullptr;
    fast_filter_skipped_ = other.fast_filter_skipped_;
    storage_datums_ = other.storage_datums_;
    snapshot_version_ = other.snapshot_version_;
    group_idx_ = other.group_idx_;
    scan_index_ = other.scan_index_;
    trans_id_ = other.trans_id_;
    mvcc_row_flag_ = other.mvcc_row_flag_;
    row_flag_ = other.row_flag_;
    count_ = other.count_;
  }
  return ret;
}

OB_DEF_SERIALIZE(ObDatumRow)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              row_flag_,
              mvcc_row_flag_,
              trans_id_,
              scan_index_,
              group_idx_,
              snapshot_version_);
  OB_UNIS_ENCODE_ARRAY(storage_datums_, count_);
  return ret;
}

OB_DEF_DESERIALIZE(ObDatumRow)
{
  int ret = OB_SUCCESS;
  fast_filter_skipped_ = false;
  have_uncommited_row_ = false;
  LST_DO_CODE(OB_UNIS_DECODE,
              row_flag_,
              mvcc_row_flag_,
              trans_id_,
              scan_index_,
              group_idx_,
              snapshot_version_);
  OB_UNIS_DECODE(count_);
  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(storage_datums_) && get_capacity() < count_) {
    if (OB_FAIL(datum_buffer_.reserve(count_))) {
      LOG_WARN("fail to reserve memory for datum buffer", K(ret), K_(count));
    }
  } else if (OB_ISNULL(storage_datums_) && OB_FAIL(init(count_))) {
    LOG_WARN("fail to init datum row", K(ret), K_(count));
  }
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE_ARRAY(storage_datums_, count_);
    fast_filter_skipped_ = false;
    have_uncommited_row_ = false;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDatumRow)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              row_flag_,
              mvcc_row_flag_,
              trans_id_,
              scan_index_,
              group_idx_,
              snapshot_version_);
  OB_UNIS_ADD_LEN_ARRAY(storage_datums_, count_);
  return len;
}

DEF_TO_STRING(ObDatumRow)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(row_flag), K_(trans_id), K_(scan_index), K_(mvcc_row_flag), K_(snapshot_version), K_(fast_filter_skipped),
      K_(have_uncommited_row), K_(group_idx), K_(count), K_(datum_buffer));
  if (NULL != buf && buf_len >= 0) {
    if (NULL != storage_datums_) {
      J_COMMA();
      J_NAME("datums");
      J_COLON();
      J_ARRAY_START();
      for (int64_t i = 0; i < count_; ++i) {
        if (0 == i) {
          databuff_printf(buf, buf_len, pos, "col_id=%ld:", i);
        } else {
          databuff_printf(buf, buf_len, pos, ", col_id=%ld:", i);
        }
        pos += storage_datums_[i].storage_to_string(buf + pos, buf_len - pos);
      }
      J_ARRAY_END();
    }
  }
  if (trans_info_) {
    databuff_printf(buf, buf_len, pos, ",trans_info[version, scn, txid, seq_no(branch_id, seq)]:%s", trans_info_);
  }
  J_OBJ_END();
  return pos;
}

bool ObDatumRow::operator==(const ObDatumRow &other) const
{
  bool is_equal = true;
  if (count_ != other.count_) {
    is_equal = false;
    STORAGE_LOG_RET(WARN, OB_INVALID_ARGUMENT, "datum row count no equal", K(other), K(*this));
  } else {
    for (int64_t i = 0; is_equal && i < count_; i++) {
      is_equal = storage_datums_[i] == other.storage_datums_[i];
      if (!is_equal) {
        STORAGE_LOG_RET(WARN, OB_ERR_UNEXPECTED, "obj and datum no equal", K(i), K(other), K(*this));
      }
    }
  }
  return is_equal;
}

bool ObDatumRow::operator==(const ObNewRow &other) const
{
  bool is_equal = true;
  if (count_ != other.count_) {
    is_equal = false;
    STORAGE_LOG_RET(WARN, OB_INVALID_ARGUMENT, "datum row count no equal", K(other), K(*this));
  } else {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; is_equal && i < count_; i++) {
      is_equal = storage_datums_[i] == other.cells_[i];
      if (!is_equal) {
        STORAGE_LOG_RET(WARN, OB_ERR_UNEXPECTED, "obj and datum no equal", K(i), K(other), K(*this));
      }
    }
  }
  return is_equal;
}

//////////////////////////////////////// ObNewRowBuilder //////////////////////////////////////////////

int ObNewRowBuilder::build(
    const blocksstable::ObDatumRow &datum_row,
    common::ObNewRow *&new_row)
{
  int ret = OB_SUCCESS;
  const int64_t col_cnt = datum_row.get_column_count();
  if (OB_UNLIKELY(nullptr == cols_descs_ || cols_descs_->count() < col_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to build new row", K(ret), K(datum_row), KP_(cols_descs));
  } else if (OB_FAIL(obj_buf_.reserve(col_cnt))) {
    STORAGE_LOG(WARN, "Failed to reserve objs", K(ret), K(col_cnt));
  } else {
    new_row_.cells_ = obj_buf_.get_data();
    new_row_.count_ = col_cnt;
    new_row_.projector_ = nullptr;
    new_row_.projector_size_ = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; i++) {
      if (OB_FAIL(datum_row.storage_datums_[i].to_obj_enhance(new_row_.cells_[i], cols_descs_->at(i).col_type_))) {
        STORAGE_LOG(WARN, "Failed to transform datum to obj", K(ret), K(i), K(datum_row.storage_datums_[i]));
      }
    }
    new_row = &new_row_;
  }
  return ret;
}

int ObNewRowBuilder::build_store_row(
    const blocksstable::ObDatumRow &datum_row,
    storage::ObStoreRow &store_row)
{
  int ret = OB_SUCCESS;
  common::ObNewRow *new_row = nullptr;
  if (OB_FAIL(build(datum_row, new_row))) {
    STORAGE_LOG(WARN, "Failed to build new row", K(ret), K(datum_row));
  } else {
    store_row.reset();
    store_row.row_val_ = *new_row;
    store_row.capacity_ = datum_row.count_;
    store_row.flag_ = datum_row.row_flag_;
    store_row.row_type_flag_ = datum_row.mvcc_row_flag_;
    store_row.trans_id_ = datum_row.trans_id_;
    store_row.scan_index_ = datum_row.scan_index_;
    store_row.group_idx_ = datum_row.group_idx_;
    store_row.snapshot_version_ = datum_row.snapshot_version_;
    store_row.fast_filter_skipped_ = datum_row.fast_filter_skipped_;
  }
  return ret;
}

int ObGhostRowUtil::is_ghost_row(
    const blocksstable::ObMultiVersionRowFlag &flag,
    bool &is_ghost_row)
{
  int ret = OB_SUCCESS;
  is_ghost_row = false;
  if (flag.is_ghost_row()) {
    is_ghost_row = true;
    if (OB_UNLIKELY(!flag.is_last_multi_version_row())) {
      ret = OB_ERR_UNEXPECTED;
      FLOG_ERROR("ghost row should only be last row", K(ret), K(flag));
    }
  }
  return ret;
}

int ObGhostRowUtil::make_ghost_row(
    const int64_t sql_sequence_col_idx,
    blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(((!row.mvcc_row_flag_.is_uncommitted_row() || !row.trans_id_.is_valid()))
                  || !row.mvcc_row_flag_.is_last_multi_version_row()
                  || row.get_column_count() < sql_sequence_col_idx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(row), K(sql_sequence_col_idx));
  } else {
    row.row_flag_.set_flag(ObDmlFlag::DF_UPDATE);
    row.mvcc_row_flag_.reset();
    row.mvcc_row_flag_.set_ghost_row(true);
    row.mvcc_row_flag_.set_last_multi_version_row(true);
    row.trans_id_.reset();
    row.storage_datums_[sql_sequence_col_idx - 1].set_int(GHOST_NUM);
    row.storage_datums_[sql_sequence_col_idx].set_int(GHOST_NUM);
    for (int i = sql_sequence_col_idx + 1; i < row.get_column_count(); ++i) {
      row.storage_datums_[i].set_nop();
    }
  }
  return ret;
}

int ObShadowRowUtil::make_shadow_row(const int64_t sql_sequence_col_idx,
                                    blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(((row.mvcc_row_flag_.is_uncommitted_row() || row.trans_id_.is_valid()))
                  || row.get_column_count() < sql_sequence_col_idx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(row), K(sql_sequence_col_idx));
  } else {
    row.storage_datums_[sql_sequence_col_idx].reuse();
    row.storage_datums_[sql_sequence_col_idx].set_int(-INT64_MAX);
    row.set_shadow_row();
  }
  return ret;
}

void format_dml_str(const int32_t flag, char *str, int len) {
  OB_ASSERT(len >= 16);
  int32_t bit;
  int32_t count = 0;
  uint32_t f = flag;
  memset(str, 0, len);
  if (f & (1<<7)) {
    strncat(str, ObDmlTypeStr[1], 16 - strlen(str));
  } else {
    strncat(str, ObDmlTypeStr[0], 16 - strlen(str));
  }
  f = f & ((1<<7) - 1);
  strncat(str, "|", 16 - strlen(str));
  strncat(str, ObDmlFlagStr[f], 16 - strlen(str));
}


void format_mvcc_str(const int32_t flag, char *str, int len) {
  OB_ASSERT(len >= 16);
  int32_t bit;
  int32_t count = 0;
  uint32_t f = flag;
  memset(str, 0, len);
  while (0 != f && count < MvccFlagCount) {
    bit = __builtin_ffs(f);
    if (0 < count) {
      strncat(str, "|", 16 - strlen(str));
    }
    strncat(str, ObMvccFlagStr[bit], 16 - strlen(str));
    f = f & (0xFFFFFFFF << bit);
    count++;
  }
}

} // namespace blocksstable
} // namespace oceanbase
