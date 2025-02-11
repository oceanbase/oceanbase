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

#include "storage/direct_load/ob_direct_load_lob_builder.h"
#include "storage/direct_load/ob_direct_load_datum_row.h"
#include "storage/direct_load/ob_direct_load_insert_lob_table_ctx.h"
#include "storage/direct_load/ob_direct_load_row_iterator.h"
#include "storage/direct_load/ob_direct_load_vector_utils.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace share;
using namespace sql;

/**
 * ObDirectLoadLobBuilder
 */

ObDirectLoadLobBuilder::ObDirectLoadLobBuilder()
  : insert_tablet_ctx_(nullptr),
    insert_lob_tablet_ctx_(nullptr),
    lob_allocator_(nullptr),
    inner_lob_allocator_("TLD_LobAlloc"),
    lob_column_idxs_(nullptr),
    lob_column_cnt_(0),
    extra_rowkey_cnt_(0),
    lob_inrow_threshold_(0),
    current_lob_slice_id_(0),
    is_closed_(false),
    is_inited_(false)
{
  inner_lob_allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadLobBuilder::~ObDirectLoadLobBuilder() {}

int ObDirectLoadLobBuilder::init(ObDirectLoadInsertTabletContext *insert_tablet_ctx,
                                 ObIAllocator *lob_allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadLobBuilder init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == insert_tablet_ctx || !insert_tablet_ctx->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(insert_tablet_ctx));
  } else if (OB_UNLIKELY(!insert_tablet_ctx->has_lob_storage() || nullptr == insert_tablet_ctx->get_lob_tablet_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected has no lob", KR(ret), KPC(insert_tablet_ctx));
  } else {
    insert_tablet_ctx_ = insert_tablet_ctx;
    insert_lob_tablet_ctx_ = insert_tablet_ctx->get_lob_tablet_ctx();
    lob_allocator_ = (nullptr != lob_allocator ? lob_allocator : &inner_lob_allocator_);
    lob_column_idxs_ = insert_tablet_ctx->get_lob_column_idxs();
    lob_column_cnt_ = lob_column_idxs_->count();
    extra_rowkey_cnt_ = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    lob_inrow_threshold_ = insert_tablet_ctx->get_lob_inrow_threshold();
    if (!insert_tablet_ctx->get_is_index_table() && OB_FAIL(init_sstable_slice_ctx())) {
      LOG_WARN("fail to init sstable slice ctx", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadLobBuilder::init_sstable_slice_ctx()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(insert_lob_tablet_ctx_->get_write_ctx(write_ctx_))) {
    LOG_WARN("fail to get write ctx", KR(ret));
  } else if (OB_FAIL(insert_lob_tablet_ctx_->open_sstable_slice(write_ctx_.start_seq_,
                                                                0/*slice_idx*/,
                                                                current_lob_slice_id_))) {
    LOG_WARN("fail to construct sstable slice", KR(ret));
  }
  return ret;
}

int ObDirectLoadLobBuilder::switch_sstable_slice()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(insert_lob_tablet_ctx_->close_sstable_slice(current_lob_slice_id_, 0/*slice_idx*/))) {
    LOG_WARN("fail to close sstable slice", KR(ret));
  } else if (OB_FAIL(init_sstable_slice_ctx())) {
    LOG_WARN("fail to init sstable slice ctx", KR(ret));
  }
  return ret;
}

int ObDirectLoadLobBuilder::check_can_skip(char *ptr, uint32_t len, bool &can_skip)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == ptr || len <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected lob column is empty data", KR(ret), KP(ptr), K(len));
  } else {
    ObLobLocatorV2 locator(ptr, len, true /*has_lob_header*/);
    if (insert_tablet_ctx_->get_is_index_table()) {
      if (!locator.is_inrow_disk_lob_locator() || len - sizeof(ObLobCommon) > lob_inrow_threshold_) {
        ret = OB_ERR_TOO_LONG_KEY_LENGTH;
        LOG_USER_ERROR(OB_ERR_TOO_LONG_KEY_LENGTH, OB_MAX_USER_ROW_KEY_LENGTH);
        STORAGE_LOG(WARN, "invalid lob", K(ret), K(locator), KP(ptr), K(len));
      } else {
        can_skip = true;
      }
    } else {
      can_skip = (locator.is_inrow_disk_lob_locator() && (len - sizeof(ObLobCommon) <= lob_inrow_threshold_));
    }
  }
  return ret;
}

int ObDirectLoadLobBuilder::check_can_skip(const ObDatumRow &datum_row, bool &can_skip)
{
  int ret = OB_SUCCESS;
  can_skip = true;
  for (int64_t i = 0; OB_SUCC(ret) && can_skip && i < lob_column_idxs_->count(); ++i) {
    const bool is_rowkey_col = lob_column_idxs_->at(i) < insert_tablet_ctx_->get_rowkey_column_count();
    const int64_t column_idx = is_rowkey_col ? lob_column_idxs_->at(i) : lob_column_idxs_->at(i) + extra_rowkey_cnt_;
    const ObDatum &datum = datum_row.storage_datums_[column_idx];
    if (datum.is_null()) {
    } else if (OB_FAIL(check_can_skip(const_cast<char *>(datum.ptr_), datum.len_, can_skip))) {
      LOG_WARN("fail to check lob can skip", KR(ret), K(column_idx), K(datum));
    }
  }
  return ret;
}

int ObDirectLoadLobBuilder::check_can_skip(const ObBatchDatumRows &datum_rows, bool &can_skip)
{
  int ret = OB_SUCCESS;
  can_skip = true;
  for (int64_t i = 0; OB_SUCC(ret) && can_skip && i < lob_column_idxs_->count(); ++i) {
    const bool is_rowkey_col = lob_column_idxs_->at(i) < insert_tablet_ctx_->get_rowkey_column_count();
    const int64_t column_idx = is_rowkey_col ? lob_column_idxs_->at(i) : lob_column_idxs_->at(i) + extra_rowkey_cnt_;
    ObIVector *vector = datum_rows.vectors_.at(column_idx);
    const VectorFormat format = vector->get_format();
    switch (format) {
      case VEC_CONTINUOUS: {
        ObContinuousBase *continuous_vec = static_cast<ObContinuousBase *>(vector);
        ObBitVector *nulls = continuous_vec->get_nulls();
        char *data = continuous_vec->get_data();
        uint32_t *offsets = continuous_vec->get_offsets();
        if (!nulls->is_all_true(datum_rows.row_count_)) {
          for (int64_t j = 0; OB_SUCC(ret) && can_skip && j < datum_rows.row_count_; ++j) {
            if (nulls->at(j)) {
            } else if (OB_FAIL(check_can_skip(data + offsets[j], offsets[j + 1] - offsets[j], can_skip))) {
              LOG_WARN("fail to check lob can skip", KR(ret), K(column_idx), K(j), KP(data),
                       K(offsets[j]), K(offsets[j + 1]));
            }
          }
        }
        break;
      }
      case VEC_DISCRETE: {
        ObDiscreteBase *discrete_vec = static_cast<ObDiscreteBase *>(vector);
        ObBitVector *nulls = discrete_vec->get_nulls();
        char **ptrs = discrete_vec->get_ptrs();
        int32_t *lens = discrete_vec->get_lens();
        if (!nulls->is_all_true(datum_rows.row_count_)) {
          for (int64_t j = 0; OB_SUCC(ret) && can_skip && j < datum_rows.row_count_; ++j) {
            if (nulls->at(j)) {
            } else if (OB_FAIL(check_can_skip(ptrs[j], lens[j], can_skip))) {
              LOG_WARN("fail to check lob can skip", KR(ret), K(column_idx), K(j), KP(ptrs[j]),
                       K(lens[j]));
            }
          }
        }
        break;
      }
      case VEC_UNIFORM: {
        ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vector);
        ObDatum *datums = uniform_vec->get_datums();
        for (int64_t j = 0; OB_SUCC(ret) && can_skip && j < datum_rows.row_count_; ++j) {
          const ObDatum &datum = datums[j];
          if (datum.is_null()) {
          } else if (OB_FAIL(check_can_skip(const_cast<char *>(datum.ptr_), datum.len_, can_skip))) {
            LOG_WARN("fail to check lob can skip", KR(ret), K(column_idx), K(j), K(datum));
          }
        }
        break;
      }
      case VEC_UNIFORM_CONST: {
        ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vector);
        const ObDatum &datum = uniform_vec->get_datums()[0];
        if (datum.is_null()) {
        } else if (OB_FAIL(check_can_skip(const_cast<char *>(datum.ptr_), datum.len_, can_skip))) {
          LOG_WARN("fail to check lob can skip", KR(ret), K(column_idx), K(datum));
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected vector format in lob column", KR(ret), K(column_idx), K(format));
        break;
    }
  }
  return ret;
}

int ObDirectLoadLobBuilder::append_row(ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  bool can_skip = false;
  lob_allocator_->reuse();
  if (OB_FAIL(check_can_skip(datum_row, can_skip))) {
    LOG_WARN("fail to check can skip", KR(ret), K(datum_row));
  } else if (can_skip) {
    // do nothing
  } else if (write_ctx_.pk_interval_.remain_count() < lob_column_cnt_ &&
             OB_FAIL(switch_sstable_slice())) {
    LOG_WARN("fail to switch sstable slice", KR(ret));
  } else if (OB_FAIL(insert_lob_tablet_ctx_->fill_lob_sstable_slice(*lob_allocator_,
                                                                current_lob_slice_id_,
                                                                write_ctx_.pk_interval_,
                                                                datum_row))) {
    LOG_WARN("fail to fill lob sstable slice", K(ret), KP(insert_lob_tablet_ctx_),
             K(current_lob_slice_id_), K(write_ctx_.pk_interval_), K(datum_row));
  }
  return ret;
}

int ObDirectLoadLobBuilder::append_batch(ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  const int64_t lob_cnt = lob_column_cnt_ * datum_rows.row_count_;
  bool can_skip = false;
  lob_allocator_->reuse();
  if (OB_FAIL(check_can_skip(datum_rows, can_skip))) {
    LOG_WARN("fail to check can skip", KR(ret), K(datum_rows));
  } else if (can_skip) {
    // do nothing
  } else if (write_ctx_.pk_interval_.remain_count() < lob_cnt && OB_FAIL(switch_sstable_slice())) {
    LOG_WARN("fail to switch sstable slice", KR(ret));
  } else if (OB_FAIL(insert_lob_tablet_ctx_->fill_lob_sstable_slice(*lob_allocator_,
                                                                current_lob_slice_id_,
                                                                write_ctx_.pk_interval_,
                                                                datum_rows))) {
    LOG_WARN("fail to fill lob sstable slice batch", K(ret), KP(insert_lob_tablet_ctx_),
             K(current_lob_slice_id_), K(write_ctx_.pk_interval_), K(datum_rows));
  }
  return ret;
}

int ObDirectLoadLobBuilder::append_lob(ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadLobBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob builder is closed", KR(ret));
  } else if (OB_FAIL(append_row(datum_row))) {
    LOG_WARN("fail to append row", KR(ret), K(datum_row));
  }
  return ret;
}

int ObDirectLoadLobBuilder::append_lob(ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadLobBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob builder is closed", KR(ret));
  } else if (OB_FAIL(append_batch(datum_rows))) {
    LOG_WARN("fail to append batch", KR(ret), K(datum_rows));
  }
  return ret;
}

int ObDirectLoadLobBuilder::fill_into_datum_row(ObDirectLoadDatumRow &datum_row,
                                                const ObDirectLoadRowFlag &row_flag)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < lob_column_idxs_->count(); ++i) {
    const int64_t column_idx = lob_column_idxs_->at(i);
    const int64_t src_column_idx = row_flag.uncontain_hidden_pk_ ? column_idx - 1 : column_idx;
    const int64_t dest_column_idx = column_idx < insert_tablet_ctx_->get_rowkey_column_count()
                                     ? column_idx : column_idx + extra_rowkey_cnt_;
    datum_row_.storage_datums_[dest_column_idx] = datum_row.storage_datums_[src_column_idx];
  }
  return ret;
}

int ObDirectLoadLobBuilder::fetch_from_datum_row(ObDirectLoadDatumRow &datum_row,
                                                 const ObDirectLoadRowFlag &row_flag)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < lob_column_idxs_->count(); ++i) {
    const int64_t column_idx = lob_column_idxs_->at(i);
    const int64_t src_column_idx = row_flag.uncontain_hidden_pk_ ? column_idx - 1 : column_idx;
    const int64_t dest_column_idx = column_idx < insert_tablet_ctx_->get_rowkey_column_count()
                                     ? column_idx : column_idx + extra_rowkey_cnt_;
    datum_row.storage_datums_[src_column_idx] = datum_row_.storage_datums_[dest_column_idx];
  }
  return ret;
}

int ObDirectLoadLobBuilder::append_lob(ObDirectLoadDatumRow &datum_row,
                                       const ObDirectLoadRowFlag &row_flag)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadLobBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob builder is closed", KR(ret));
  } else if (!datum_row_.is_valid() && OB_FAIL(insert_tablet_ctx_->init_datum_row(datum_row_))) {
    LOG_WARN("fail to init datum row", KR(ret));
  } else {
    if (OB_FAIL(fill_into_datum_row(datum_row, row_flag))) {
      LOG_WARN("fail to fill into datum row", KR(ret));
    } else if (OB_FAIL(append_row(datum_row_))) {
      LOG_WARN("fail to append row", KR(ret), K(datum_row_));
    } else if (OB_FAIL(fetch_from_datum_row(datum_row, row_flag))) {
      LOG_WARN("fail to fetch from datum row", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadLobBuilder::fill_into_datum_row(const IVectorPtrs &vectors,
                                                const int64_t row_idx,
                                                const ObDirectLoadRowFlag &row_flag)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < lob_column_idxs_->count(); ++i) {
    const int64_t column_idx = lob_column_idxs_->at(i);
    const int64_t src_column_idx = row_flag.uncontain_hidden_pk_ ? column_idx - 1 : column_idx;
    const int64_t dest_column_idx = column_idx < insert_tablet_ctx_->get_rowkey_column_count()
                                     ? column_idx : column_idx + extra_rowkey_cnt_;
    ObIVector *vector = vectors.at(src_column_idx);
    ObDatum &datum = datum_row_.storage_datums_[dest_column_idx];
    if (OB_FAIL(ObDirectLoadVectorUtils::to_datum(vector, row_idx, datum))) {
      LOG_WARN("fail to get datum", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadLobBuilder::fetch_from_datum_row(const IVectorPtrs &vectors,
                                                 const int64_t row_idx,
                                                 const ObDirectLoadRowFlag &row_flag)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < lob_column_idxs_->count(); ++i) {
    const int64_t column_idx = lob_column_idxs_->at(i);
    const int64_t src_column_idx = row_flag.uncontain_hidden_pk_ ? column_idx - 1 : column_idx;
    const int64_t dest_column_idx = column_idx < insert_tablet_ctx_->get_rowkey_column_count()
                                     ? column_idx : column_idx + extra_rowkey_cnt_;
    ObIVector *vector = vectors.at(src_column_idx);
    ObDatum &datum = datum_row_.storage_datums_[dest_column_idx];
    if (OB_FAIL(ObDirectLoadVectorUtils::set_datum(vector, row_idx, datum))) {
      LOG_WARN("fail to set datum", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadLobBuilder::append_lob(const IVectorPtrs &vectors,
                                       const int64_t row_idx,
                                       const ObDirectLoadRowFlag &row_flag)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadLobBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob builder is closed", KR(ret));
  } else if (!datum_row_.is_valid() && OB_FAIL(insert_tablet_ctx_->init_datum_row(datum_row_))) {
    LOG_WARN("fail to init datum row", KR(ret));
  } else {
    if (OB_FAIL(fill_into_datum_row(vectors, row_idx, row_flag))) {
      LOG_WARN("fail to fill into datum row", KR(ret));
    } else if (OB_FAIL(append_row(datum_row_))) {
      LOG_WARN("fail to append row", KR(ret), K(datum_row_));
    } else if (OB_FAIL(fetch_from_datum_row(vectors, row_idx, row_flag))) {
      LOG_WARN("fail to fetch from datum row", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadLobBuilder::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadLobBuilder not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet lob builder is closed", KR(ret));
  } else {
    if (!insert_tablet_ctx_->get_is_index_table() && OB_FAIL(insert_lob_tablet_ctx_->close_sstable_slice(current_lob_slice_id_, 0/*slice_idx*/))) {
      LOG_WARN("fail to close sstable slice ", KR(ret));
    } else {
      current_lob_slice_id_ = 0;
      is_closed_ = true;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
