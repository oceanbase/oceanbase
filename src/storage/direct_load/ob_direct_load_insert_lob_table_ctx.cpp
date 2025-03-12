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

#include "storage/direct_load/ob_direct_load_insert_lob_table_ctx.h"
#include "storage/direct_load/ob_direct_load_insert_data_table_ctx.h"

namespace oceanbase
{
namespace storage
{
using namespace blocksstable;
using namespace common;
using namespace share;
using namespace table;

/**
 * ObDirectLoadInsertLobTabletContext
 */

ObDirectLoadInsertLobTabletContext::ObDirectLoadInsertLobTabletContext() : data_tablet_ctx_(nullptr)
{
}

ObDirectLoadInsertLobTabletContext::~ObDirectLoadInsertLobTabletContext() {}

int ObDirectLoadInsertLobTabletContext::init(ObDirectLoadInsertLobTableContext *table_ctx,
                                             ObDirectLoadInsertDataTabletContext *data_tablet_ctx,
                                             const ObLSID &ls_id,
                                             const ObTabletID &origin_tablet_id,
                                             const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadInsertDataTabletContext init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == table_ctx || nullptr == data_tablet_ctx || !ls_id.is_valid() ||
                         !origin_tablet_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_ctx), KP(data_tablet_ctx), K(ls_id),
             K(origin_tablet_id), K(tablet_id));
  } else {
    table_ctx_ = table_ctx;
    param_ = &table_ctx->param_;
    data_tablet_ctx_ = data_tablet_ctx;
    ls_id_ = ls_id;
    origin_tablet_id_ = origin_tablet_id;
    tablet_id_ = tablet_id;
    pk_tablet_id_ = tablet_id_; // 从目标表取
    tablet_id_in_lob_id_ =
      tablet_id_; // 与ObDirectLoadSliceWriter::fill_lob_into_macro_block中的取值保持一致
    if (OB_FAIL(start_seq_.set_parallel_degree(param_->reserved_parallel_))) {
      LOG_WARN("fail to set parallel degree", KR(ret), K(param_->reserved_parallel_));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadInsertLobTabletContext::open()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertLobTabletContext not init", KR(ret), KP(this));
  } else if (OB_FAIL(data_tablet_ctx_->open())) {
    LOG_WARN("fail to open", KR(ret));
  }
  return ret;
}

int ObDirectLoadInsertLobTabletContext::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertLobTabletContext not init", KR(ret), KP(this));
  } else if (OB_FAIL(data_tablet_ctx_->close())) {
    LOG_WARN("fail to close", KR(ret));
  }
  return ret;
}

void ObDirectLoadInsertLobTabletContext::cancel()
{
  if (nullptr != data_tablet_ctx_) {
    data_tablet_ctx_->cancel();
  }
}

//////////////////////// write interface ////////////////////////

int ObDirectLoadInsertLobTabletContext::get_pk_interval(uint64_t count,
                                                        ObTabletCacheInterval &pk_interval)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDirectLoadInsertTabletContext::get_pk_interval(count, pk_interval))) {
    LOG_WARN("fail to get pk interval", KR(ret));
  }
  // set min_insert_lob_id_
  else if (!min_insert_lob_id_.is_valid()) {
    uint64_t value = 0;
    if (OB_FAIL(pk_interval.get_value(value))) {
      LOG_WARN("fail to get value", KR(ret), K(pk_interval));
      ret = OB_ERR_UNEXPECTED; // rewrite error code
    } else if (OB_UNLIKELY(value == 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected lob id zero", KR(ret), K(pk_interval));
    } else {
      min_insert_lob_id_.tablet_id_ = tablet_id_in_lob_id_.id();
      ObLobManager::transform_lob_id(value, min_insert_lob_id_.lob_id_);
    }
  }
  return ret;
}

int ObDirectLoadInsertLobTabletContext::open_sstable_slice(const ObMacroDataSeq &start_seq,
                                                           const int64_t slice_idx,
                                                           int64_t &slice_id)
{
  UNUSED(slice_idx);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertLobTabletContext not init", KR(ret), KP(this));
  } else if (OB_FAIL(data_tablet_ctx_->open_lob_sstable_slice(start_seq, slice_id))) {
    LOG_WARN("fail to open lob sstabel slice", KR(ret));
  }
  return ret;
}

int ObDirectLoadInsertLobTabletContext::fill_sstable_slice(const int64_t &slice_id,
                                                           ObIStoreRowIterator &iter,
                                                           int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertLobTabletContext not init", KR(ret), KP(this));
  } else if (OB_FAIL(
               data_tablet_ctx_->fill_lob_meta_sstable_slice(slice_id, iter, affected_rows))) {
    LOG_WARN("fail to fill lob meta sstable slice", KR(ret));
  }
  return ret;
}

int ObDirectLoadInsertLobTabletContext::fill_sstable_slice(const int64_t &slice_id,
                                                           const ObBatchDatumRows &datum_rows)
{
  return OB_ERR_UNEXPECTED;
}

int ObDirectLoadInsertLobTabletContext::fill_lob_sstable_slice(ObIAllocator &allocator,
                                                               const int64_t &lob_slice_id,
                                                               ObTabletCacheInterval &pk_interval,
                                                               ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertLobTabletContext not init", KR(ret), KP(this));
  } else if (OB_FAIL(data_tablet_ctx_->fill_lob_sstable_slice(allocator, lob_slice_id, pk_interval,
                                                              datum_row))) {
    LOG_WARN("fail to fill lob sstable slice", KR(ret));
  }
  return ret;
}

int ObDirectLoadInsertLobTabletContext::fill_lob_sstable_slice(ObIAllocator &allocator,
                                                               const int64_t &lob_slice_id,
                                                               ObTabletCacheInterval &pk_interval,
                                                               ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertLobTabletContext not init", KR(ret), KP(this));
  } else if (OB_FAIL(data_tablet_ctx_->fill_lob_sstable_slice(allocator, lob_slice_id, pk_interval,
                                                              datum_rows))) {
    LOG_WARN("fail to fill lob sstable slice", KR(ret));
  }
  return ret;
}

int ObDirectLoadInsertLobTabletContext::close_sstable_slice(const int64_t slice_id,
                                                            const int64_t slice_idx)
{
  UNUSED(slice_idx);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertLobTabletContext not init", KR(ret), KP(this));
  } else if (OB_FAIL(data_tablet_ctx_->close_lob_sstable_slice(slice_id))) {
    LOG_WARN("fail to close lob sstabel slice", KR(ret));
  }
  return ret;
}

/**
 * ObDirectLoadInsertLobTableContext
 */

ObDirectLoadInsertLobTableContext::ObDirectLoadInsertLobTableContext() {}

ObDirectLoadInsertLobTableContext::~ObDirectLoadInsertLobTableContext() {}

int ObDirectLoadInsertLobTableContext::init(
  const ObDirectLoadInsertTableParam &param, ObDirectLoadInsertDataTableContext *data_table_ctx,
  const ObIArray<ObTableLoadLSIdAndPartitionId> &ls_partition_ids,
  const ObIArray<ObTableLoadLSIdAndPartitionId> &target_ls_partition_ids,
  const ObIArray<ObTableLoadLSIdAndPartitionId> &data_ls_partition_ids)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadInsertDataTableContext init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid() || nullptr == data_table_ctx ||
                         ls_partition_ids.empty() || target_ls_partition_ids.empty() ||
                         ls_partition_ids.count() != target_ls_partition_ids.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param), KP(data_table_ctx), K(ls_partition_ids),
             K(target_ls_partition_ids));
  } else if (OB_FAIL(inner_init())) {
    LOG_WARN("fail to inner init", KR(ret));
  } else {
    param_ = param;
    if (OB_FAIL(create_all_tablet_contexts(data_table_ctx, ls_partition_ids,
                                           target_ls_partition_ids, data_ls_partition_ids))) {
      LOG_WARN("fail to create all tablet contexts", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadInsertLobTableContext::create_all_tablet_contexts(
  ObDirectLoadInsertDataTableContext *data_table_ctx,
  const ObIArray<ObTableLoadLSIdAndPartitionId> &ls_partition_ids,
  const ObIArray<ObTableLoadLSIdAndPartitionId> &target_ls_partition_ids,
  const ObIArray<ObTableLoadLSIdAndPartitionId> &data_ls_partition_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_partition_ids.count(); ++i) {
    const ObTabletID &origin_tablet_id = ls_partition_ids.at(i).part_tablet_id_.tablet_id_;
    const ObLSID &ls_id = target_ls_partition_ids.at(i).ls_id_;
    const ObTabletID &tablet_id = target_ls_partition_ids.at(i).part_tablet_id_.tablet_id_;
    const ObTabletID &data_tablet_id = data_ls_partition_ids.at(i).part_tablet_id_.tablet_id_;
    ObDirectLoadInsertTabletContext *data_tablet_ctx = nullptr;
    ObDirectLoadInsertLobTabletContext *lob_tablet_ctx = nullptr;
    if (OB_FAIL(data_table_ctx->get_tablet_context(data_tablet_id, data_tablet_ctx))) {
      LOG_WARN("fail to get tablet ctx", KR(ret), K(data_tablet_id));
    } else if (OB_ISNULL(lob_tablet_ctx =
                           OB_NEWx(ObDirectLoadInsertLobTabletContext, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadInsertLobTabletContext", KR(ret));
    } else if (OB_FAIL(lob_tablet_ctx->init(
                 this, static_cast<ObDirectLoadInsertDataTabletContext *>(data_tablet_ctx), ls_id,
                 origin_tablet_id, tablet_id))) {
      LOG_WARN("fail to init tablet ctx", KR(ret));
    } else if (OB_FAIL(tablet_ctx_map_.set_refactored(origin_tablet_id, lob_tablet_ctx))) {
      LOG_WARN("fail to set tablet ctx map", KR(ret));
    } else {
      data_tablet_ctx->set_lob_tablet_ctx(lob_tablet_ctx);
    }
    if (OB_FAIL(ret)) {
      if (nullptr != lob_tablet_ctx) {
        lob_tablet_ctx->~ObDirectLoadInsertLobTabletContext();
        allocator_.free(lob_tablet_ctx);
        lob_tablet_ctx = nullptr;
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
