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
#include "storage/direct_load/ob_direct_load_vector_utils.h"
#include "storage/direct_load/ob_direct_load_i_table.h"

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
    is_table_without_pk_(false),
    column_count_(0),
    store_column_count_(0),
    non_partitioned_tablet_id_vector_(nullptr),
    tablet_id_set_(),
    pre_sort_writer_(nullptr),
    batch_ctx_(nullptr),
    row_count_(0),
    last_check_status_cycle_(0),
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
  is_table_without_pk_ = false;
  column_count_ = 0;
  store_column_count_ = 0;
  non_partitioned_tablet_id_vector_ = nullptr;
  if (tablet_id_set_.created()) {
    tablet_id_set_.destroy();
  }
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

    is_table_without_pk_ = store_ctx_->ctx_->schema_.is_table_without_pk_;
    column_count_ = store_ctx_->ctx_->schema_.store_column_count_;
    store_column_count_ = (is_table_without_pk_ ? column_count_ - 1 : column_count_);
    if (!store_ctx_->ctx_->schema_.is_partitioned_table_) {
      non_partitioned_tablet_id_vector_ =
        store_ctx_->ctx_->schema_.non_partitioned_tablet_id_vector_;
    }
    if (OB_FAIL(init_tablet_id_set())) {
      LOG_WARN("fail to init tablet id set", KR(ret));
    } else if (store_ctx_->write_ctx_.enable_pre_sort_) {
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
  } else if (OB_UNLIKELY(column_ids.count() != column_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(column_ids), K(column_ids.count()), K(column_count_));
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
    const ObIArray<ObColDesc> &col_descs = store_ctx_->ctx_->schema_.column_descs_;
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
  const ObIArray<ObColDesc> &col_descs = store_ctx_->ctx_->schema_.column_descs_;
  const int64_t max_batch_size = store_ctx_->ctx_->param_.batch_size_;
  if (OB_UNLIKELY(nullptr != batch_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected batch ctx not null", KR(ret));
  } else if (OB_ISNULL(batch_ctx_ = OB_NEWx(BatchCtx, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new BatchCtx", KR(ret));
  } else {
    batch_ctx_->max_batch_size_ = max_batch_size;
    batch_ctx_->batch_vectors_.set_block_allocator(ModulePageAllocator(allocator_));
    batch_ctx_->const_vectors_.set_block_allocator(ModulePageAllocator(allocator_));
    batch_ctx_->append_vectors_.set_block_allocator(ModulePageAllocator(allocator_));
    batch_ctx_->heap_vectors_.set_block_allocator(ModulePageAllocator(allocator_));
    // non-vectorized
    if (!is_vectorized) {
      batch_ctx_->row_flag_.uncontain_hidden_pk_ = is_table_without_pk_;
      if (OB_FAIL(ObDirectLoadVectorUtils::new_vector(VEC_FIXED,
                                                      ObDirectLoadVectorUtils::tablet_id_value_tc,
                                                      allocator_,
                                                      batch_ctx_->tablet_id_batch_vector_))) {
        LOG_WARN("fail to new tablet id fixed vector", KR(ret));
      } else if (OB_FAIL(ObDirectLoadVectorUtils::prepare_vector(batch_ctx_->tablet_id_batch_vector_,
                                                                 max_batch_size,
                                                                 allocator_))) {
        LOG_WARN("fail to prepare tablet id fixed vector", KR(ret), K(max_batch_size));
      } else if (OB_FAIL(batch_ctx_->batch_buffer_.init(col_descs, max_batch_size))) {
        LOG_WARN("fail to init batch buffer", KR(ret), K(col_descs), K(max_batch_size));
      }
    }
    // vectorized
    else {
      if (!use_rich_format) { // vectorized 1.0
        // 向量化1.0需要把ObDatumVector转成ObVector
        if (OB_FAIL(ObDirectLoadVectorUtils::new_vector(VEC_UNIFORM,
                                                        ObDirectLoadVectorUtils::tablet_id_value_tc,
                                                        allocator_,
                                                        batch_ctx_->tablet_id_batch_vector_))) {
          LOG_WARN("fail to new tablet id uniform vector", KR(ret));
        } else if (OB_FAIL(ObDirectLoadVectorUtils::new_vector(VEC_UNIFORM_CONST,
                                                               ObDirectLoadVectorUtils::tablet_id_value_tc,
                                                               allocator_,
                                                               batch_ctx_->tablet_id_const_vector_))) {
          LOG_WARN("fail to new tablet id uniform const vector", KR(ret));
        }
      } else { // vectorized 2.0
        // 向量化2.0拿到的就是ObVector, do nothing
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(batch_ctx_->col_fixed_ = to_bit_vector(
                             allocator_.alloc(ObBitVector::memory_size(column_count_))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObBitVector", KR(ret), K(column_count_));
      } else if (OB_ISNULL(batch_ctx_->col_lob_storage_ = to_bit_vector(
                             allocator_.alloc(ObBitVector::memory_size(column_count_))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObBitVector", KR(ret), K(column_count_));
      } else if (OB_FAIL(batch_ctx_->batch_vectors_.prepare_allocate(column_count_))) {
        LOG_WARN("fail to prepare allocate", KR(ret), K(column_count_));
      } else if (OB_FAIL(batch_ctx_->const_vectors_.prepare_allocate(column_count_))) {
        LOG_WARN("fail to prepare allocate", KR(ret), K(column_count_));
      } else if (OB_FAIL(batch_ctx_->append_vectors_.prepare_allocate(column_count_))) {
        LOG_WARN("fail to prepare allocate", KR(ret), K(column_count_));
      } else {
        batch_ctx_->col_fixed_->reset(column_count_);
        batch_ctx_->col_lob_storage_->reset(column_count_);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < column_count_; ++i) {
        const ObColDesc &col_desc = col_descs.at(i);
        const int16_t precision = col_desc.col_type_.is_decimal_int()
                                    ? col_desc.col_type_.get_stored_precision()
                                    : PRECISION_UNKNOWN_YET;
        VecValueTypeClass value_tc = get_vec_value_tc(col_desc.col_type_.get_type(),
                                                      col_desc.col_type_.get_scale(),
                                                      precision);
        const bool is_fixed = is_fixed_length_vec(value_tc);
        const bool is_lob_storage = col_desc.col_type_.is_lob_storage();
        ObIVector *batch_vector = nullptr;
        ObIVector *const_vector = nullptr;
        if (is_fixed) {
          batch_ctx_->col_fixed_->set(i);
        }
        if (is_lob_storage) {
          batch_ctx_->col_lob_storage_->set(i);
        }
        if (!use_rich_format) { // vectorized 1.0
          // 向量化1.0传入的是ObDatumVector, 需要转成ObVector, 都需要构造vector
          //  * fixed col不用浅拷贝, 不用分配vector成员内存
          //  * unfixed col要浅拷贝, 需要分配vector成员内存, lob不用构造const_vector
          if (OB_FAIL(ObDirectLoadVectorUtils::new_vector(VEC_UNIFORM,
                                                          value_tc,
                                                          allocator_,
                                                          batch_vector))) {
            LOG_WARN("fail to new uniform vector", KR(ret), K(i), K(col_desc), K(value_tc));
          } else if (!is_fixed && OB_FAIL(ObDirectLoadVectorUtils::prepare_vector(batch_vector,
                                                                                  max_batch_size,
                                                                                  allocator_))) {
            LOG_WARN("fail to prepare uniform vector", KR(ret), K(i), K(col_desc), K(max_batch_size));
          } else if (is_lob_storage) {
            // lob不用构造const_vector
          } else if (OB_FAIL(ObDirectLoadVectorUtils::new_vector(VEC_UNIFORM_CONST,
                                                                 value_tc,
                                                                 allocator_,
                                                                 const_vector))) {
            LOG_WARN("fail to new uniform const vector", KR(ret), K(i), K(col_desc), K(value_tc));
          } else if (!is_fixed && OB_FAIL(ObDirectLoadVectorUtils::prepare_vector(const_vector,
                                                                                  max_batch_size,
                                                                                  allocator_))) {
            LOG_WARN("fail to prepare uniform const vector", KR(ret), K(i), K(col_desc), K(max_batch_size));
          }
        } else { // vectorized 2.0
          // 向量化2.0传入的是ObVector
          //  * fixed col直接用传入的ObVector, 不用构造vector
          //  * unfixed col需要浅拷贝, 需要构造vector并分配vector成员内存, lob不用构造const_vector
          if (is_fixed) {
          } else if (OB_FAIL(ObDirectLoadVectorUtils::new_vector(VEC_DISCRETE,
                                                                 value_tc,
                                                                 allocator_,
                                                                 batch_vector))) {
            LOG_WARN("fail to new discrete vector", KR(ret), K(i), K(col_desc), K(value_tc));
          } else if (OB_FAIL(ObDirectLoadVectorUtils::prepare_vector(batch_vector,
                                                                     max_batch_size,
                                                                     allocator_))) {
            LOG_WARN("fail to prepare discrete vector", KR(ret), K(i), K(col_desc), K(max_batch_size));
          } else if (is_lob_storage) {
            // lob不用构造const_vector
          } else if (OB_FAIL(ObDirectLoadVectorUtils::new_vector(VEC_UNIFORM_CONST,
                                                                 value_tc,
                                                                 allocator_,
                                                                 const_vector))) {
            LOG_WARN("fail to new uniform const vector", KR(ret), K(i), K(col_desc), K(value_tc));
          } else if (OB_FAIL(ObDirectLoadVectorUtils::prepare_vector(const_vector,
                                                                     max_batch_size,
                                                                     allocator_))) {
            LOG_WARN("fail to prepare uniform const vector", KR(ret), K(i), K(col_desc), K(max_batch_size));
          }
        }
        if (OB_SUCC(ret)) {
          batch_ctx_->batch_vectors_.at(i) = batch_vector;
          batch_ctx_->const_vectors_.at(i) = const_vector;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (is_table_without_pk_ &&
          OB_FAIL(batch_ctx_->heap_vectors_.prepare_allocate(store_column_count_))) {
        LOG_WARN("fail to prepare allocate", KR(ret), K(store_column_count_));
      }
    }
  }
  return ret;
}

int ObTableLoadStoreTransPXWriter::init_tablet_id_set()
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 1024;
  ObMemAttr attr(MTL_ID(), "TLD_TabletIdSet");
  if (OB_FAIL(tablet_id_set_.create(bucket_num, attr))) {
    LOG_WARN("fail to create hashset", KR(ret));
  } else {
    const ObIArray<ObTableLoadLSIdAndPartitionId> &ls_partition_ids =
      store_ctx_->data_store_table_ctx_->ls_partition_ids_;
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_partition_ids.count(); ++i) {
      const ObTabletID &tablet_id = ls_partition_ids.at(i).part_tablet_id_.tablet_id_;
      if (OB_FAIL(tablet_id_set_.set_refactored(tablet_id))) {
        LOG_WARN("fail to set hashset", KR(ret), K(i), K(tablet_id), K(ls_partition_ids));
        if (OB_LIKELY(OB_HASH_EXIST == ret)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected duplicate tablet id", KR(ret), K(i), K(tablet_id),
                   K(ls_partition_ids));
        }
      }
    }
  }
  return ret;
}

int ObTableLoadStoreTransPXWriter::check_tablet(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ret = tablet_id_set_.exist_refactored(tablet_id);
  if (OB_LIKELY(OB_HASH_EXIST == ret)) {
    ret = OB_SUCCESS;
  } else if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
    ret = OB_TABLET_NOT_EXIST;
    LOG_WARN("tablet id not exist", KR(ret), K(tablet_id),
             K(store_ctx_->data_store_table_ctx_->ls_partition_ids_));
  } else {
    LOG_WARN("fail to check exist tablet id", KR(ret));
  }
  return ret;
}

int ObTableLoadStoreTransPXWriter::check_tablet(ObIVector *vector, const ObBatchRows &batch_rows)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < batch_rows.size_; ++i) {
    if (!batch_rows.all_rows_active_ && batch_rows.skip_->at(i)) {
      continue;
    } else {
      const ObTabletID tablet_id = ObDirectLoadVectorUtils::get_tablet_id(vector, i);
      if (OB_FAIL(check_tablet(tablet_id))) {
        LOG_WARN("fail to check tablet", KR(ret), K(i), K(tablet_id));
      }
    }
  }
  return ret;
}

int ObTableLoadStoreTransPXWriter::check_tablet(const ObDatumVector &datum_vector,
                                                const ObBatchRows &batch_rows)
{
  int ret = OB_SUCCESS;
  if (datum_vector.is_batch()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_rows.size_; ++i) {
      if (!batch_rows.all_rows_active_ && batch_rows.skip_->at(i)) {
        continue;
      } else {
        const ObDatum &datum = datum_vector.datums_[i];
        const ObTabletID tablet_id(datum.get_int());
        if (OB_FAIL(check_tablet(tablet_id))) {
          LOG_WARN("fail to check tablet", KR(ret), K(i), K(datum), K(tablet_id));
        }
      }
    }
  } else {
    const ObDatum &datum = datum_vector.datums_[0];
    const ObTabletID tablet_id(datum.get_int());
    if (OB_FAIL(check_tablet(tablet_id))) {
      LOG_WARN("fail to check tablet", KR(ret), K(datum), K(tablet_id));
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

int ObTableLoadStoreTransPXWriter::write_vector(ObIVector *tablet_id_vector,
                                                const ObIArray<ObIVector *> &vectors,
                                                const ObBatchRows &batch_rows,
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
  } else if (OB_UNLIKELY(vectors.count() != column_count_ || nullptr == batch_rows.skip_ ||
                         batch_rows.size_ <= 0 || batch_rows.size_ > batch_ctx_->max_batch_size_ ||
                         batch_rows.end_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(column_count_), K(batch_ctx_->max_batch_size_),
             K(vectors.count()), K(batch_rows));
  } else if (nullptr != tablet_id_vector && OB_FAIL(check_tablet(tablet_id_vector, batch_rows))) {
    LOG_WARN("fail to check tablet", KR(ret), KP(tablet_id_vector));
  } else {
    if (nullptr == tablet_id_vector) {
      tablet_id_vector = non_partitioned_tablet_id_vector_;
    }
    batch_ctx_->brs_.skip_ = batch_rows.skip_;
    batch_ctx_->brs_.size_ = batch_rows.size_;
    batch_ctx_->brs_.all_rows_active_ =
      (batch_rows.all_rows_active_ || 0 == batch_rows.skip_->accumulate_bit_cnt(batch_rows.size_));
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count_; ++i) {
      if (batch_ctx_->col_fixed_->at(i)) {
        batch_ctx_->append_vectors_.at(i) = vectors.at(i);
      } else if (batch_ctx_->col_lob_storage_->at(i)) {
        ObIVector *vector = batch_ctx_->batch_vectors_.at(i);
        if (VEC_UNIFORM_CONST != vectors.at(i)->get_format()) {
          // 浅拷贝
          if (OB_FAIL(ObDirectLoadVectorUtils::shallow_copy_vector(vectors.at(i),
                                                                   vector,
                                                                   batch_rows.size_))) {
            LOG_WARN("fail to shallow copy vector", KR(ret), K(i));
          }
        } else {
          // 展开
          if (OB_FAIL(ObDirectLoadVectorUtils::expand_const_vector(vectors.at(i),
                                                                   vector,
                                                                   batch_rows.size_))) {
            LOG_WARN("fail to expand const vector", KR(ret), K(i));
          }
        }
        batch_ctx_->append_vectors_.at(i) = vector;
      } else { // 浅拷贝
        ObIVector *vector = VEC_UNIFORM_CONST != vectors.at(i)->get_format()
                              ? batch_ctx_->batch_vectors_.at(i)
                              : batch_ctx_->const_vectors_.at(i);
        if (OB_FAIL(ObDirectLoadVectorUtils::shallow_copy_vector(vectors.at(i), vector,
                                                                 batch_rows.size_))) {
          LOG_WARN("fail to shallow copy vector", KR(ret), K(i));
        } else {
          batch_ctx_->append_vectors_.at(i) = vector;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(flush_batch(tablet_id_vector, batch_ctx_->append_vectors_, batch_ctx_->brs_, affected_rows))) {
      LOG_WARN("fail to flush batch", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadStoreTransPXWriter::write_batch(const ObDatumVector &tablet_id_datum_vector,
                                               const ObIArray<ObDatumVector> &datum_vectors,
                                               const ObBatchRows &batch_rows,
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
  } else if (OB_UNLIKELY(datum_vectors.count() != column_count_ || nullptr == batch_rows.skip_ ||
                         batch_rows.size_ <= 0 || batch_rows.size_ > batch_ctx_->max_batch_size_ ||
                         batch_rows.end_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(column_count_), K(batch_ctx_->max_batch_size_),
             K(datum_vectors), K(batch_rows));
  } else if (nullptr != tablet_id_datum_vector.datums_ &&
             OB_FAIL(check_tablet(tablet_id_datum_vector, batch_rows))) {
    LOG_WARN("fail to check tablet", KR(ret), K(tablet_id_datum_vector));
  } else {
    ObIVector *tablet_id_vector = nullptr;
    if (nullptr != tablet_id_datum_vector.datums_) {
      tablet_id_vector = tablet_id_datum_vector.is_batch() ? batch_ctx_->tablet_id_batch_vector_
                                                           : batch_ctx_->tablet_id_const_vector_;
      static_cast<ObUniformBase *>(tablet_id_vector)->set_datums(tablet_id_datum_vector.datums_);
    } else {
      tablet_id_vector = non_partitioned_tablet_id_vector_;
    }
    batch_ctx_->brs_.skip_ = batch_rows.skip_;
    batch_ctx_->brs_.size_ = batch_rows.size_;
    batch_ctx_->brs_.all_rows_active_ =
      (batch_rows.all_rows_active_ || 0 == batch_rows.skip_->accumulate_bit_cnt(batch_rows.size_));
    for (int64_t i = 0; i < column_count_; ++i) {
      const ObDatumVector &datum_vector = datum_vectors.at(i);
      ObIVector *vector = nullptr;
      if (batch_ctx_->col_lob_storage_->at(i)) {
        vector = batch_ctx_->batch_vectors_.at(i);
        if (datum_vector.is_batch()) {
          // 浅拷贝
          MEMCPY(static_cast<ObUniformBase *>(vector)->get_datums(),
                datum_vector.datums_,
                sizeof(ObDatum) * batch_rows.size_);
        } else {
          // 展开
          if (OB_FAIL(ObDirectLoadVectorUtils::expand_const_datum(datum_vector.datums_[0],
                                                                  vector,
                                                                  batch_rows.size_))) {
            LOG_WARN("fail to expand const vector", KR(ret), K(i));
          }
        }
      } else {
        vector = datum_vector.is_batch() ? batch_ctx_->batch_vectors_.at(i)
                                         : batch_ctx_->const_vectors_.at(i);
        if (batch_ctx_->col_fixed_->at(i)) {
          static_cast<ObUniformBase *>(vector)->set_datums(datum_vector.datums_);
        } else { // 浅拷贝
          MEMCPY(static_cast<ObUniformBase *>(vector)->get_datums(),
                 datum_vector.datums_,
                 sizeof(ObDatum) * (datum_vector.is_batch() ? batch_rows.size_ : 1));
        }
      }
      batch_ctx_->append_vectors_.at(i) = vector;
    }
    if (OB_FAIL(flush_batch(tablet_id_vector, batch_ctx_->append_vectors_, batch_ctx_->brs_, affected_rows))) {
      LOG_WARN("fail to flush batch", KR(ret));
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
  } else if (OB_FAIL(check_tablet(tablet_id))) {
    LOG_WARN("fail to check tablet", KR(ret), K(tablet_id));
  } else {
    bool is_full = false;
    const int64_t batch_idx = batch_ctx_->batch_buffer_.get_row_count();
    batch_ctx_->datum_row_.storage_datums_ = is_table_without_pk_ ? row.storage_datums_ + 1 : row.storage_datums_;
    batch_ctx_->datum_row_.count_ = is_table_without_pk_ ? row.count_ - 1 : row.count_;
    if (OB_FAIL(ObDirectLoadVectorUtils::set_tablet_id(batch_ctx_->tablet_id_batch_vector_,
                                                       batch_idx,
                                                       tablet_id))) {
      LOG_WARN("fail to set tablet id", KR(ret));
    } else if (OB_FAIL(batch_ctx_->batch_buffer_.append_row(batch_ctx_->datum_row_,
                                                            batch_ctx_->row_flag_,
                                                            is_full))) {
      LOG_WARN("fail to append row", KR(ret));
    } else if (is_full) {
      if (OB_FAIL(flush_buffer())) {
        LOG_WARN("fail to flush buffer", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadStoreTransPXWriter::flush_buffer()
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  batch_ctx_->brs_.size_ = batch_ctx_->batch_buffer_.get_row_count();
  batch_ctx_->brs_.all_rows_active_ = true;
  if (OB_FAIL(flush_batch(batch_ctx_->tablet_id_batch_vector_,
                          batch_ctx_->batch_buffer_.get_vectors(),
                          batch_ctx_->brs_,
                          affected_rows))) {
    LOG_WARN("fail to flush batch", KR(ret));
  } else if (OB_UNLIKELY(affected_rows != batch_ctx_->brs_.size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected row count", KR(ret), K(affected_rows), K(batch_ctx_->brs_.size_));
  } else {
    batch_ctx_->batch_buffer_.reuse();
  }
  return ret;
}

int ObTableLoadStoreTransPXWriter::flush_batch(ObIVector *tablet_id_vector,
                                               const ObIArray<ObIVector *> &vectors,
                                               const ObBatchRows &batch_rows,
                                               int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  if (OB_UNLIKELY(nullptr == tablet_id_vector || vectors.count() != column_count_ ||
                  (!batch_rows.all_rows_active_ && nullptr == batch_rows.skip_) ||
                  batch_rows.size_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(column_count_), KP(tablet_id_vector), K(vectors.count()),
             K(batch_rows));
  } else {
    const ObIArray<ObIVector *> *append_vectors = nullptr;
    if (is_table_without_pk_) {
      for (int64_t i = 0; i < store_column_count_; ++i) {
        batch_ctx_->heap_vectors_.at(i) = vectors.at(i + 1);
      }
      append_vectors = &batch_ctx_->heap_vectors_;
    } else {
      append_vectors = &vectors;
    }
    if (nullptr != pre_sort_writer_) {
      if (OB_FAIL(pre_sort_writer_->px_write(tablet_id_vector, *append_vectors, batch_rows, affected_rows))) {
        LOG_WARN("fail to px write", KR(ret));
      }
    } else {
      if (OB_FAIL(writer_->px_write(tablet_id_vector, *append_vectors, batch_rows, affected_rows))) {
        LOG_WARN("fail to px write", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      row_count_ += affected_rows;
      if (OB_FAIL(check_status())) {
        LOG_WARN("fail to check status", KR(ret));
      }
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
  } else if (nullptr != batch_ctx_ && !batch_ctx_->batch_buffer_.empty() &&
             OB_FAIL(flush_buffer())) {
    LOG_WARN("fail to flush buffer", KR(ret));
  } else if (nullptr != pre_sort_writer_ && OB_FAIL(pre_sort_writer_->close())) {
    LOG_WARN("fail to push chunk", KR(ret));
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
