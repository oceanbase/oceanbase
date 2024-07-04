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
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_trans.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_trans_store.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace share::schema;
using namespace table;

ObTableLoadStoreTransPXWriter::ObTableLoadStoreTransPXWriter()
  : store_ctx_(nullptr),
    trans_(nullptr),
    writer_(nullptr),
    column_count_(0),
    row_count_(0),
    is_heap_table_(false),
    can_write_(false),
    is_inited_(false)
{
}

ObTableLoadStoreTransPXWriter::~ObTableLoadStoreTransPXWriter()
{
  reset();
}

void ObTableLoadStoreTransPXWriter::reset()
{
  is_inited_ = false;
  can_write_ = false;
  is_heap_table_ = false;
  column_count_ = 0;
  row_count_ = 0;
  if (nullptr != store_ctx_) {
    if (nullptr != trans_) {
      if (nullptr != writer_) {
        trans_->put_store_writer(writer_);
        writer_ = nullptr;
      }
      store_ctx_->put_trans(trans_);
      trans_ = nullptr;
    }
    ATOMIC_AAF(&store_ctx_->px_writer_count_, -1);
    store_ctx_ = nullptr;
  }
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
    ATOMIC_AAF(&store_ctx_->px_writer_count_, 1);
    if (OB_SUCC(check_status())) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadStoreTransPXWriter::prepare_write(const ObTabletID &tablet_id,
                                                 const ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreTransPXWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || column_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id), K(column_ids));
  } else if (OB_UNLIKELY(can_write_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("already can write", KR(ret), KPC(this));
  } else {
    if (OB_FAIL(check_tablet(tablet_id))) {
      LOG_WARN("fail to check tablet", KR(ret));
    } else if (OB_FAIL(check_columns(column_ids))) {
      LOG_WARN("fail to check columns", KR(ret));
    } else {
      tablet_id_ = tablet_id;
      column_count_ = column_ids.count();
      is_heap_table_ = store_ctx_->ctx_->schema_.is_heap_table_;
      can_write_ = true;
    }
  }
  return ret;
}

int ObTableLoadStoreTransPXWriter::check_tablet(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id));
  } else if (OB_ISNULL(store_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected store ctx is null", KR(ret), KPC(this));
  } else {
    bool tablet_found = false;
    for (int64_t i = 0; i < store_ctx_->ls_partition_ids_.count(); ++i) {
      const ObTableLoadLSIdAndPartitionId &ls_part_id = store_ctx_->ls_partition_ids_.at(i);
      if (ls_part_id.part_tablet_id_.tablet_id_ == tablet_id) {
        tablet_found = true;
        break;
      }
    }
    if (OB_UNLIKELY(!tablet_found)) {
      ret = OB_TABLET_NOT_EXIST;
      LOG_WARN("tablet id not found", KR(ret), K(tablet_id), K(store_ctx_->ls_partition_ids_));
    }
  }
  return ret;
}

int ObTableLoadStoreTransPXWriter::check_columns(const ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  ObTableLoadTableCtx *table_ctx = nullptr;
  if (OB_UNLIKELY(column_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(column_ids));
  } else if (OB_ISNULL(store_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected store ctx is null", KR(ret), KPC(this));
  } else if (OB_ISNULL(table_ctx = store_ctx_->ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table ctx is null", KR(ret), KPC(store_ctx_));
  } else {
    if (OB_UNLIKELY(table_ctx->schema_.column_descs_.count() != column_ids.count())) {
      ret = OB_SCHEMA_NOT_UPTODATE;
      LOG_WARN("column count not match", KR(ret), K(table_ctx->schema_.column_descs_),
               K(column_ids));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
      const ObColDesc &col_desc = table_ctx->schema_.column_descs_.at(i);
      const uint64_t column_id = column_ids.at(i);
      if (OB_UNLIKELY(col_desc.col_id_ != column_id)) {
        ret = OB_SCHEMA_NOT_UPTODATE;
        LOG_WARN("column id not match", KR(ret), K(i), K(col_desc), K(column_id),
                 K(table_ctx->schema_.column_descs_), K(column_ids));
      }
    }
  }
  return ret;
}

int ObTableLoadStoreTransPXWriter::check_status()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(store_ctx_->check_status(ObTableLoadStatusType::LOADING))) {
    LOG_WARN("fail to check status", KR(ret));
  } else if (OB_FAIL(trans_->check_trans_status(ObTableLoadTransStatusType::RUNNING))) {
    LOG_WARN("fail to check trans status", KR(ret));
  }
  return ret;
}

int ObTableLoadStoreTransPXWriter::write(const ObNewRow &row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreTransPXWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!can_write_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not write", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!row.is_valid() || row.count_ != column_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(row), K(column_count_));
  } else {
    ObNewRow new_row;
    if (is_heap_table_) {
      new_row.assign(row.cells_ + 1, row.count_ - 1);
    } else {
      new_row.assign(row.cells_, row.count_);
    }
    if (OB_FAIL(writer_->px_write(tablet_id_, new_row))) {
      LOG_WARN("fail to px write", KR(ret), K(row), K(new_row));
    } else {
      row_count_++;
      if (row_count_ % CHECK_STATUS_CYCLE == 0) {
        if (OB_FAIL(check_status())) {
          LOG_WARN("fail to check status", KR(ret));
        }
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
