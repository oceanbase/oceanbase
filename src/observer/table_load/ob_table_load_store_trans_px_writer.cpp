/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_store_trans_px_writer.h"
#include "observer/table_load/ob_table_load_pre_sort_writer.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/ob_table_load_store_trans.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_trans_store.h"
#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_vector.h"
#include "storage/direct_load/ob_direct_load_vector_utils.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace share::schema;
using namespace sql;
using namespace table;

ObTableLoadStoreTransPXWriter::ObTableLoadStoreTransPXWriter()
  : allocator_("TLD_PXWriter"),
    store_ctx_(nullptr),
    trans_(nullptr),
    writer_(nullptr),
    column_count_(0),
    single_tablet_id_vector_(nullptr),
    pre_sort_writer_(nullptr),
    batch_ctx_(nullptr),
    row_count_(0),
    last_check_status_cycle_(0),
    is_single_part_(false),
    is_vectorized_(false),
    use_rich_format_(false),
    can_write_(false),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObTableLoadStoreTransPXWriter::~ObTableLoadStoreTransPXWriter()
{
  reset();
}

void ObTableLoadStoreTransPXWriter::reset()
{
  is_inited_ = false;
  can_write_ = false;
  if (nullptr != store_ctx_) {
    if (nullptr != trans_) {
      if (nullptr != writer_) {
        trans_->put_store_writer(writer_);
        writer_ = nullptr;
      }
      store_ctx_->put_trans(trans_);
      trans_ = nullptr;
    }
    ATOMIC_AAF(&store_ctx_->write_ctx_.px_writer_cnt_, -1);
    store_ctx_ = nullptr;
  }
  column_count_ = 0;
  single_tablet_id_.reset();
  single_tablet_id_vector_ = nullptr;
  if (nullptr != pre_sort_writer_) {
    pre_sort_writer_->~ObTableLoadPreSortWriter();
    allocator_.free(pre_sort_writer_);
    pre_sort_writer_ = nullptr;
  }
  if (nullptr != batch_ctx_) {
    batch_ctx_->~BatchCtx();
    allocator_.free(batch_ctx_);
    batch_ctx_ = nullptr;
  }
  row_count_ = 0;
  last_check_status_cycle_ = 0;
  is_single_part_ = false;
  is_vectorized_ = false;
  use_rich_format_ = false;
  allocator_.reset();
}
int ObTableLoadStoreTransPXWriter::init(ObTableLoadStoreCtx *store_ctx,
                                        ObTableLoadStoreTrans *trans,
                                        ObTableLoadTransStoreWriter *writer)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadStoreTransPXWriter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == store_ctx || nullptr == trans || nullptr == writer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(store_ctx), KP(trans), KP(writer));
  } else {
    store_ctx_ = store_ctx;
    trans_ = trans;
    writer_ = writer;
    trans_->inc_ref_count();
    writer_->inc_ref_count();
    ATOMIC_AAF(&store_ctx_->write_ctx_.px_writer_cnt_, 1);

    column_count_ = store_ctx_->write_ctx_.px_column_descs_.count();
    is_single_part_ = store_ctx_->write_ctx_.is_single_part_;
    if (is_single_part_) {
      single_tablet_id_ = store_ctx_->write_ctx_.single_tablet_id_;
      single_tablet_id_vector_ = store_ctx_->write_ctx_.single_tablet_id_vector_;
    }
    if (store_ctx_->write_ctx_.enable_pre_sort_) {
      if (OB_ISNULL(pre_sort_writer_ = OB_NEWx(ObTableLoadPreSortWriter, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObTableLoadPreSortWriter", KR(ret));
      } else if (OB_FAIL(pre_sort_writer_->init(store_ctx_->write_ctx_.pre_sorter_,
                                                writer_,
                                                store_ctx_->error_row_handler_))) {
        LOG_WARN("fail to init pre sort wirter", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadStoreTransPXWriter::prepare_write(const ObIArray<uint64_t> &column_ids,
                                                 const bool is_vectorized,
                                                 const bool use_rich_format)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreTransPXWriter not init", KR(ret), KP(this));
  }
  // 不要在这里检查column_ids.count()与column_count_的关系
  // 旁路导入与DDL并发的场景, 两个值就是可能不一样的
  else if (OB_UNLIKELY(column_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(column_ids));
  } else if (OB_UNLIKELY(can_write_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("already can write", KR(ret), KPC(this));
  } else {
    if (OB_FAIL(check_columns(column_ids))) {
      LOG_WARN("fail to check columns", KR(ret));
    } else if (OB_FAIL(init_batch_ctx(is_vectorized, use_rich_format))) {
      LOG_WARN("fail to init batch ctx", KR(ret), K(is_vectorized), K(use_rich_format));
    } else {
      is_vectorized_ = is_vectorized;
      use_rich_format_ = use_rich_format;
      can_write_ = true;
    }
  }
  return ret;
}

int ObTableLoadStoreTransPXWriter::check_columns(const ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(column_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(column_ids));
  } else {
    const ObIArray<ObColDesc> &col_descs = store_ctx_->write_ctx_.px_column_descs_;
    if (OB_UNLIKELY(col_descs.count() != column_ids.count())) {
      ret = OB_SCHEMA_NOT_UPTODATE;
      LOG_WARN("column count not match", KR(ret), K(col_descs), K(column_ids));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
      const ObColDesc &col_desc = col_descs.at(i);
      const uint64_t column_id = column_ids.at(i);
      if (OB_UNLIKELY(col_desc.col_id_ != column_id)) {
        ret = OB_SCHEMA_NOT_UPTODATE;
        LOG_WARN("column id not match", KR(ret), K(i), K(col_desc), K(column_id), K(col_descs),
                 K(column_ids));
      }
    }
  }
  return ret;
}

int ObTableLoadStoreTransPXWriter::init_batch_ctx(const bool is_vectorized,
                                                  const bool use_rich_format)
{
  int ret = OB_SUCCESS;
  const int64_t max_batch_size = store_ctx_->ctx_->param_.batch_size_;
  const int64_t tablet_cnt = store_ctx_->write_ctx_.tablet_idx_map_.size();
  const ObDirectLoadRowFlag &row_flag = store_ctx_->write_ctx_.table_data_desc_.row_flag_;
  if (OB_UNLIKELY(nullptr != batch_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected batch ctx not null", KR(ret));
  } else if (OB_ISNULL(batch_ctx_ = OB_NEWx(BatchCtx, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new BatchCtx", KR(ret));
  } else {
    batch_ctx_->max_batch_size_ = max_batch_size;
    batch_ctx_->max_bytes_size_ = DEFAULT_MAX_BYTES_SIZE;
    batch_ctx_->row_flag_ = row_flag;
    if (OB_FAIL(ObDirectLoadVector::create_vector(VEC_FIXED,
                                                  ObDirectLoadVectorUtils::tablet_id_value_tc,
                                                  false /*is_nullable*/,
                                                  max_batch_size,
                                                  allocator_,
                                                  batch_ctx_->tablet_id_vector_))) {
      LOG_WARN("fail to create tablet id fixed vector", KR(ret));
   } else if (OB_FAIL(batch_ctx_->batch_rows_.init(store_ctx_->write_ctx_.px_column_descs_,
                                                   store_ctx_->write_ctx_.px_column_project_idxs_,
                                                   store_ctx_->ctx_->schema_.column_descs_,
                                                   store_ctx_->ctx_->schema_.col_nullables_,
                                                   row_flag,
                                                   max_batch_size))) {
      LOG_WARN("fail to init batch rows", KR(ret));
    } else if (OB_ISNULL(batch_ctx_->selector_ = static_cast<uint16_t *>(
                           allocator_.alloc(sizeof(uint16_t) * max_batch_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem", KR(ret), K(max_batch_size));
    } else if (OB_ISNULL(batch_ctx_->tablet_offsets_ = static_cast<uint16_t *>(
                           allocator_.alloc(sizeof(uint16_t) * (tablet_cnt + 1))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem", KR(ret), K(tablet_cnt));
    }
  }
  return ret;
}

int ObTableLoadStoreTransPXWriter::check_status()
{
  int ret = OB_SUCCESS;
  const int64_t cycle = row_count_ / CHECK_STATUS_CYCLE;
  if (cycle > last_check_status_cycle_) {
    last_check_status_cycle_ = cycle;
    if (OB_FAIL(store_ctx_->check_status(ObTableLoadStatusType::LOADING))) {
      LOG_WARN("fail to check status", KR(ret));
    } else if (OB_FAIL(trans_->check_trans_status(ObTableLoadTransStatusType::RUNNING))) {
      LOG_WARN("fail to check trans status", KR(ret));
    }
  }
  return ret;
}

inline void ObTableLoadStoreTransPXWriter::make_selector(const ObBatchRows &brs,
                                                         uint16_t *selector,
                                                         int64_t &size)
{
  size = 0;
  for (int64_t i = 0; i < brs.size_; ++i) {
    if (brs.skip_->at(i)) {
      continue;
    } else {
      selector[size++] = i;
    }
  }
}

int ObTableLoadStoreTransPXWriter::adapt_vectors(const ObIArray<ObIVector *> &vectors,
                                                 const ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < vectors.count(); ++i) {
    ObIVector *vec = vectors.at(i);
    const VectorFormat format = vec->get_format();
    switch (format) {
      case VEC_FIXED:
      case VEC_CONTINUOUS:
      case VEC_UNIFORM:
      case VEC_UNIFORM_CONST:
        break;
      case VEC_DISCRETE: {
        ObDiscreteBase *discrete_vec = static_cast<ObDiscreteBase *>(vec);
        if (discrete_vec->has_null()) {
          // 将null对应的len设置为0
          ObBitVector *nulls = discrete_vec->get_nulls();
          ObLength *lens = discrete_vec->get_lens();
          for (int64_t i = 0; i < brs.size_; ++i) {
            lens[i] *= !nulls->at(i);
          }
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected vector format", KR(ret), KPC(this), KPC(vec), K(format));
        break;
    }
  }
  return ret;
}

int ObTableLoadStoreTransPXWriter::write_vector(ObIVector *tablet_id_vector,
                                                const ObIArray<ObIVector *> &vectors,
                                                const ObBatchRows &brs,
                                                int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreTransPXWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!can_write_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not write", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!is_vectorized_ || !use_rich_format_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected write vector", KR(ret), K(is_vectorized_), K(use_rich_format_));
  } else if (OB_UNLIKELY((!is_single_part_ && nullptr == tablet_id_vector) ||
                         vectors.count() != column_count_ || nullptr == brs.skip_ ||
                         brs.size_ <= 0 || brs.size_ > batch_ctx_->max_batch_size_ || brs.end_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(is_single_part_), K(column_count_),
             K(batch_ctx_->max_batch_size_), KP(tablet_id_vector), K(vectors.count()), K(brs));
  } else if (OB_FAIL(adapt_vectors(vectors, brs))) {
    LOG_WARN("fail to adapt vectors", KR(ret));
  } else {
    const bool all_rows_active =
      (brs.all_rows_active_ || 0 == brs.skip_->accumulate_bit_cnt(brs.size_));
    if (all_rows_active) {
      // 单分区场景, 检查分区是否一致
      if (is_single_part_ && OB_UNLIKELY(!ObDirectLoadVectorUtils::check_is_same_tablet_id(
                               single_tablet_id_, tablet_id_vector, brs.size_))) {
        ret = OB_TABLET_NOT_EXIST;
        LOG_WARN("unexpected tablet id not same", KR(ret), K(single_tablet_id_),
                 KPC(tablet_id_vector));
      }
      // buffer不为空则先刷下去
      else if (!batch_ctx_->batch_rows_.empty() && OB_FAIL(flush_buffer())) {
        LOG_WARN("fail to flush buffer", KR(ret));
      }
      // 浅拷贝数据直接刷下去
      else if (!is_single_part_ &&
               OB_FAIL(batch_ctx_->tablet_id_vector_->shallow_copy(tablet_id_vector, brs.size_))) {
        LOG_WARN("fail to shallow copy", KR(ret));
      } else if (OB_FAIL(batch_ctx_->batch_rows_.shallow_copy(vectors, brs.size_))) {
        LOG_WARN("fail to shallow copy vectors", KR(ret));
      } else if (OB_FAIL(flush_buffer())) {
        LOG_WARN("fail to flush buffer", KR(ret));
      } else {
        affected_rows = brs.size_;
      }
    } else {
      uint16_t *selector = batch_ctx_->selector_;
      int64_t size = 0;
      make_selector(brs, batch_ctx_->selector_, size);
      // 单分区场景, 检查分区是否一致
      if (is_single_part_ && OB_UNLIKELY(!ObDirectLoadVectorUtils::check_is_same_tablet_id(
                               single_tablet_id_, tablet_id_vector, selector, size))) {
        ret = OB_TABLET_NOT_EXIST;
        LOG_WARN("unexpected tablet id not same", KR(ret), K(single_tablet_id_),
                 KPC(tablet_id_vector));
      }
      while (OB_SUCC(ret) && size > 0) {
        const int64_t append_size = MIN(size, batch_ctx_->batch_rows_.remain_size());
        const int64_t batch_idx = batch_ctx_->batch_rows_.size();
        if (!is_single_part_ &&
            OB_FAIL(batch_ctx_->tablet_id_vector_->append_selective(batch_idx,
                                                                    tablet_id_vector,
                                                                    selector,
                                                                    append_size))) {
          LOG_WARN("fail to append selective", KR(ret));
        } else if (OB_FAIL(batch_ctx_->batch_rows_.append_selective(vectors, selector, append_size))) {
          LOG_WARN("fail to append selective", KR(ret));
        } else {
          size -= append_size;
          selector += append_size;
          if (OB_FAIL(flush_buffer_if_need())) {
            LOG_WARN("fail to flush buffer", KR(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        affected_rows = size;
      }
    }
  }
  return ret;
}

int ObTableLoadStoreTransPXWriter::write_batch(const ObDatumVector &tablet_id_datum_vector,
                                               const ObIArray<ObDatumVector> &datum_vectors,
                                               const ObBatchRows &brs,
                                               int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreTransPXWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!can_write_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not write", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!is_vectorized_ || use_rich_format_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected write batch", KR(ret), K(is_vectorized_), K(use_rich_format_));
  } else if (OB_UNLIKELY((!is_single_part_ && nullptr == tablet_id_datum_vector.datums_) ||
                         datum_vectors.count() != column_count_ || nullptr == brs.skip_ ||
                         brs.size_ <= 0 || brs.size_ > batch_ctx_->max_batch_size_ || brs.end_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(is_single_part_), K(column_count_),
             K(batch_ctx_->max_batch_size_), K(tablet_id_datum_vector), K(datum_vectors), K(brs));
  } else {
    const bool all_rows_active =
      brs.all_rows_active_ || 0 == brs.skip_->accumulate_bit_cnt(brs.size_);
    if (all_rows_active) {
      // 单分区场景, 检查分区是否一致
      if (is_single_part_ && OB_UNLIKELY(!ObDirectLoadVectorUtils::check_is_same_tablet_id(
                               single_tablet_id_, tablet_id_datum_vector, brs.size_))) {
        ret = OB_TABLET_NOT_EXIST;
        LOG_WARN("unexpected tablet id not same", KR(ret), K(single_tablet_id_),
                 K(tablet_id_datum_vector));
      }
      // buffer不为空则先刷下去
      else if (!batch_ctx_->batch_rows_.empty() && OB_FAIL(flush_buffer())) {
        LOG_WARN("fail to flush buffer", KR(ret));
      }
      // 浅拷贝数据直接刷下去
      else if (!is_single_part_ &&
               OB_FAIL(batch_ctx_->tablet_id_vector_->shallow_copy(tablet_id_datum_vector, brs.size_))) {
        LOG_WARN("fail to shallow copy", KR(ret));
      } else if (OB_FAIL(batch_ctx_->batch_rows_.shallow_copy(datum_vectors, brs.size_))) {
        LOG_WARN("fail to shallow copy datum vectors", KR(ret));
      } else if (OB_FAIL(flush_buffer())) {
        LOG_WARN("fail to flush buffer", KR(ret));
      } else {
        affected_rows = brs.size_;
      }
    } else {
      uint16_t *selector = batch_ctx_->selector_;
      int64_t size = 0;
      make_selector(brs, batch_ctx_->selector_, size);
      // 单分区场景, 检查分区是否一致
      if (is_single_part_ && OB_UNLIKELY(!ObDirectLoadVectorUtils::check_is_same_tablet_id(
                               single_tablet_id_, tablet_id_datum_vector, selector, size))) {
        ret = OB_TABLET_NOT_EXIST;
        LOG_WARN("unexpected tablet id not same", KR(ret), K(single_tablet_id_),
                 K(tablet_id_datum_vector));
      }
      while (OB_SUCC(ret) && size > 0) {
        const int64_t append_size = MIN(size, batch_ctx_->batch_rows_.remain_size());
        const int64_t batch_idx = batch_ctx_->batch_rows_.size();
        if (!is_single_part_ &&
            OB_FAIL(batch_ctx_->tablet_id_vector_->append_selective(batch_idx,
                                                                    tablet_id_datum_vector,
                                                                    selector,
                                                                    append_size))) {
          LOG_WARN("fail to append selective", KR(ret));
        } else if (OB_FAIL(batch_ctx_->batch_rows_.append_selective(datum_vectors, selector, append_size))) {
          LOG_WARN("fail to append selective", KR(ret));
        } else {
          size -= append_size;
          selector += append_size;
          if (OB_FAIL(flush_buffer_if_need())) {
            LOG_WARN("fail to flush buffer", KR(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        affected_rows = size;
      }
    }
  }
  return ret;
}

int ObTableLoadStoreTransPXWriter::write_row(const ObTabletID &tablet_id, const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreTransPXWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!can_write_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not write", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(is_vectorized_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected write row", KR(ret), K(is_vectorized_), K(use_rich_format_));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || !row.is_valid() || row.count_ != column_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id), K(row), K(column_count_));
  } else {
    const int64_t batch_idx = batch_ctx_->batch_rows_.size();
    batch_ctx_->datum_row_.storage_datums_ = row.storage_datums_;
    batch_ctx_->datum_row_.count_ = row.count_;
    ObDatum tablet_id_datum(reinterpret_cast<const char *>(&tablet_id), sizeof(ObTabletID), false);
    if (is_single_part_ && OB_UNLIKELY(tablet_id != single_tablet_id_)) {
      ret = OB_TABLET_NOT_EXIST;
      LOG_WARN("unexpected tablet id not same", KR(ret), K(single_tablet_id_), K(tablet_id));
    } else if (OB_FAIL(batch_ctx_->tablet_id_vector_->append_datum(batch_idx, tablet_id_datum))) {
      LOG_WARN("fail to append datum", KR(ret));
    } else if (OB_FAIL(batch_ctx_->batch_rows_.append_row(batch_ctx_->datum_row_))) {
      LOG_WARN("fail to append row", KR(ret));
    } else if (OB_FAIL(flush_buffer_if_need())) {
      LOG_WARN("fail to flush buffer", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadStoreTransPXWriter::flush_buffer_if_need()
{
  int ret = OB_SUCCESS;
  if (batch_ctx_->batch_rows_.full() ||
      batch_ctx_->batch_rows_.bytes_usage() >= batch_ctx_->max_bytes_size_) {
    if (OB_FAIL(flush_buffer())) {
      LOG_WARN("fail to flush buffer", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadStoreTransPXWriter::check_tablets(ObDirectLoadVector *tablet_id_vector,
                                                 const int64_t size)
{
  int ret = OB_SUCCESS;
  const ObTableLoadStoreWriteCtx::TabletIdxMap &tablet_idx_map =
    store_ctx_->write_ctx_.tablet_idx_map_;
  const uint64_t *tablet_ids = reinterpret_cast<uint64_t *>(
    static_cast<ObFixedLengthBase *>(tablet_id_vector->get_vector())->get_data());
  for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
    const uint64_t tablet_id = tablet_ids[i];
    const int64_t *tablet_idx = tablet_idx_map.get(tablet_id);
    if (OB_ISNULL(tablet_idx)) {
      ret = OB_TABLET_NOT_EXIST;
      LOG_WARN("unexpected tablet id not found", KR(ret), K(tablet_id));
    }
  }
  return ret;
}

int ObTableLoadStoreTransPXWriter::flush_buffer()
{
  int ret = OB_SUCCESS;
  ObDirectLoadBatchRows &batch_rows = batch_ctx_->batch_rows_.get_batch_rows();
  if (store_ctx_->ctx_->schema_.has_lob_rowkey_ &&
      OB_FAIL(ObDirectLoadVectorUtils::check_rowkey_length(
        batch_rows, store_ctx_->ctx_->schema_.rowkey_column_count_))) {
    LOG_WARN("fail to check rowkey length", KR(ret));
  } else if (nullptr != pre_sort_writer_) { // pre_sort
    ObIVector *tablet_id_vector =
      is_single_part_ ? single_tablet_id_vector_ : batch_ctx_->tablet_id_vector_->get_vector();
    if (!is_single_part_ &&
        OB_FAIL(check_tablets(batch_ctx_->tablet_id_vector_, batch_rows.size()))) {
      LOG_WARN("fail to check tablets", KR(ret));
    } else if (OB_FAIL(pre_sort_writer_->px_write(tablet_id_vector, batch_rows))) {
      LOG_WARN("fail to px write", KR(ret));
    }
  } else if (is_single_part_) { // 单分区场景
    if (OB_FAIL(writer_->px_write(single_tablet_id_, batch_rows))) {
      LOG_WARN("fail to px write", KR(ret));
    }
  } else { // 多分区场景
    const ObTableLoadStoreWriteCtx::TabletIdxMap &tablet_idx_map =
      store_ctx_->write_ctx_.tablet_idx_map_;
    const int64_t size = batch_rows.size();
    const uint64_t *tablet_ids = reinterpret_cast<uint64_t *>(
      static_cast<ObFixedLengthBase *>(batch_ctx_->tablet_id_vector_->get_vector())->get_data());
    const bool all_tablet_id_is_same =
      ObDirectLoadVectorUtils::check_all_tablet_id_is_same(tablet_ids, size);
    if (all_tablet_id_is_same) { // 数据属于同一个分区
      const uint64_t tablet_id = tablet_ids[0];
      const int64_t *tablet_idx = tablet_idx_map.get(tablet_id);
      if (OB_ISNULL(tablet_idx)) {
        ret = OB_TABLET_NOT_EXIST;
        LOG_WARN("unexpected tablet id not found", KR(ret), K(tablet_id));
      } else if (OB_FAIL(writer_->px_write(ObTabletID(tablet_id), batch_rows))) {
        LOG_WARN("fail to px write", KR(ret));
      }
    } else { // 数据属于多个分区
      const int64_t tablet_cnt = tablet_idx_map.size();
      uint16_t *selector = batch_ctx_->selector_;
      uint16_t *tablet_offsets = batch_ctx_->tablet_offsets_;
      MEMSET(selector, 0, sizeof(uint16_t) * batch_ctx_->max_batch_size_);
      MEMSET(tablet_offsets, 0, sizeof(uint16_t) * (tablet_cnt + 1));
      for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
        const uint64_t tablet_id = tablet_ids[i];
        const int64_t *tablet_idx = tablet_idx_map.get(tablet_id);
        if (OB_ISNULL(tablet_idx)) {
          ret = OB_TABLET_NOT_EXIST;
          LOG_WARN("unexpected tablet id not found", KR(ret), K(tablet_id));
        } else {
          tablet_offsets[*tablet_idx]++;
        }
      }
      for (int64_t i = 1; i <= tablet_cnt; ++i) {
        tablet_offsets[i] += tablet_offsets[i - 1];
      }
      for (int i = size - 1; OB_SUCC(ret) && i >= 0; --i) {
        const uint64_t tablet_id = tablet_ids[i];
        const int64_t *tablet_idx = tablet_idx_map.get(tablet_id);
        selector[tablet_offsets[*tablet_idx] - 1] = i;
        tablet_offsets[*tablet_idx]--;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_cnt; ++i) {
        const int64_t start = tablet_offsets[i];
        const int64_t size = tablet_offsets[i + 1] - start;
        if (size > 0) {
          const uint64_t tablet_id = tablet_ids[selector[start]];
          if (OB_FAIL(writer_->px_write(ObTabletID(tablet_id),
                                        batch_rows,
                                        selector + start,
                                        size))) {
            LOG_WARN("fail to px write", KR(ret));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    row_count_ += batch_rows.size();
    batch_ctx_->batch_rows_.reuse();
    if (OB_FAIL(check_status())) {
      LOG_WARN("fail to check status", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadStoreTransPXWriter::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreTransPXWriter not init", KR(ret), KP(this));
  } else if (nullptr != batch_ctx_ && !batch_ctx_->batch_rows_.empty() && OB_FAIL(flush_buffer())) {
    LOG_WARN("fail to flush buffer", KR(ret));
  } else if (nullptr != pre_sort_writer_ && OB_FAIL(pre_sort_writer_->close())) {
    LOG_WARN("fail to push chunk", KR(ret));
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
