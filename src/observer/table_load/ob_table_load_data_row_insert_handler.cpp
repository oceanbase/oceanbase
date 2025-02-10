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
#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_data_row_insert_handler.h"
#include "observer/table_load/ob_table_load_error_row_handler.h"
#include "observer/table_load/ob_table_load_index_table_builder.h"
#include "observer/table_load/ob_table_load_lob_table_builder.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "storage/direct_load/ob_direct_load_external_row.h"
#include "storage/direct_load/ob_direct_load_multiple_datum_row.h"

namespace oceanbase
{
namespace observer
{
using namespace blocksstable;
using namespace common;
using namespace storage;

ObTableLoadDataRowInsertHandler::ObTableLoadDataRowInsertHandler()
  : store_ctx_(nullptr), error_row_handler_(nullptr), result_info_(nullptr), is_inited_(false)
{
}

ObTableLoadDataRowInsertHandler::~ObTableLoadDataRowInsertHandler() {}

int ObTableLoadDataRowInsertHandler::init(ObTableLoadStoreCtx *store_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    int ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadDataRowInsertHandler init twice", KR(ret), KP(this));
  } else if (OB_ISNULL(store_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(store_ctx));
  } else {
    store_ctx_ = store_ctx;
    error_row_handler_ = store_ctx->error_row_handler_;
    result_info_ = &store_ctx->result_info_;
    dup_action_ = store_ctx->ctx_->param_.dup_action_;
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadDataRowInsertHandler::handle_insert_row(const ObTabletID &tablet_id,
                                                       const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataRowInsertHandler not init", KR(ret), KP(this));
  } else {
    if (ObDirectLoadMethod::is_incremental(store_ctx_->ctx_->param_.method_)) {
      // 构造索引数据
      for (int64_t i = 0; OB_SUCC(ret) && i < store_ctx_->index_store_table_ctxs_.count(); ++i) {
        ObTableLoadStoreIndexTableCtx *index_table_ctx = store_ctx_->index_store_table_ctxs_.at(i);
        ObTableLoadIndexTableBuilder *index_builder = nullptr;
        if (OB_FAIL(index_table_ctx->get_insert_table_builder(index_builder))) {
          LOG_WARN("fail to get index table builder", KR(ret));
        } else if (OB_FAIL(index_builder->append_insert_row(tablet_id, datum_row))) {
          LOG_WARN("fail to append insert row", KR(ret), K(tablet_id), K(datum_row));
        }
      }
    }
    ATOMIC_INC(&result_info_->rows_affected_);
  }
  return ret;
}

int ObTableLoadDataRowInsertHandler::handle_insert_row(const ObTabletID &tablet_id,
                                                       const ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataRowInsertHandler not init", KR(ret), KP(this));
  } else {
    if (ObDirectLoadMethod::is_incremental(store_ctx_->ctx_->param_.method_)) {
      // 构造索引数据
      for (int64_t i = 0; OB_SUCC(ret) && i < store_ctx_->index_store_table_ctxs_.count(); ++i) {
        ObTableLoadStoreIndexTableCtx *index_table_ctx = store_ctx_->index_store_table_ctxs_.at(i);
        ObTableLoadIndexTableBuilder *index_builder = nullptr;
        if (OB_FAIL(index_table_ctx->get_insert_table_builder(index_builder))) {
          LOG_WARN("fail to get index table builder", KR(ret));
        } else if (OB_FAIL(index_builder->append_insert_row(tablet_id, datum_row))) {
          LOG_WARN("fail to append insert row", KR(ret), K(tablet_id), K(datum_row));
        }
      }
    }
    ATOMIC_INC(&result_info_->rows_affected_);
  }
  return ret;
}

int ObTableLoadDataRowInsertHandler::handle_insert_batch(const ObTabletID &tablet_id,
                                                         const ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataRowInsertHandler not init", KR(ret), KP(this));
  } else if (0 == datum_rows.row_count_) {
    // do nothing
  } else {
    if (ObDirectLoadMethod::is_incremental(store_ctx_->ctx_->param_.method_)) {
      // 构造索引数据
      for (int64_t i = 0; OB_SUCC(ret) && i < store_ctx_->index_store_table_ctxs_.count(); ++i) {
        ObTableLoadStoreIndexTableCtx *index_table_ctx = store_ctx_->index_store_table_ctxs_.at(i);
        ObTableLoadIndexTableBuilder *index_builder = nullptr;
        if (OB_FAIL(index_table_ctx->get_insert_table_builder(index_builder))) {
          LOG_WARN("fail to get index table builder", KR(ret));
        } else if (OB_FAIL(index_builder->append_insert_batch(tablet_id, datum_rows))) {
          LOG_WARN("fail to append insert batch", KR(ret), K(tablet_id), K(datum_rows));
        }
      }
    }
    ATOMIC_AAF(&result_info_->rows_affected_, datum_rows.row_count_);
  }
  return ret;
}

int ObTableLoadDataRowInsertHandler::handle_update_row(const ObTabletID &tablet_id,
                                                       const ObDirectLoadDatumRow &datum_row)
{
  UNUSED(tablet_id);
  UNUSED(datum_row);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (ObLoadDupActionType::LOAD_STOP_ON_DUP == dup_action_) {
      if (OB_FAIL(error_row_handler_->handle_error_row(OB_ERR_PRIMARY_KEY_DUPLICATE))) {
        LOG_WARN("fail to handle error row", KR(ret));
      }
    } else if (ObLoadDupActionType::LOAD_REPLACE == dup_action_) {
      ATOMIC_AAF(&result_info_->rows_affected_, 2);
      ATOMIC_INC(&result_info_->deleted_);
    } else if (ObLoadDupActionType::LOAD_IGNORE == dup_action_) {
      ATOMIC_INC(&result_info_->skipped_);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected dup action", KR(ret), K_(dup_action));
    }
  }
  return ret;
}

int ObTableLoadDataRowInsertHandler::handle_update_row(
  const ObTabletID &tablet_id,
  ObArray<const ObDirectLoadExternalRow *> &rows,
  const ObDirectLoadExternalRow *&row)
{
  UNUSED(tablet_id);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(rows.count() < 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret));
  } else {
    const int64_t duplicate_row_count = rows.count() - 1;
    struct
    {
      bool operator()(const ObDirectLoadExternalRow *lhs,
                      const ObDirectLoadExternalRow *rhs)
      {
        return lhs->seq_no_ < rhs->seq_no_;
      }
    } external_row_compare;
    lib::ob_sort(rows.begin(), rows.end(), external_row_compare);
    if (ObLoadDupActionType::LOAD_STOP_ON_DUP == dup_action_) {
      if (OB_FAIL(error_row_handler_->handle_error_row(OB_ERR_PRIMARY_KEY_DUPLICATE,
                                                       duplicate_row_count))) {
        LOG_WARN("fail to handle error row", KR(ret));
      } else {
        row = rows.at(0);
      }
    } else if (ObLoadDupActionType::LOAD_REPLACE == dup_action_) {
      ATOMIC_AAF(&result_info_->rows_affected_, 2 * duplicate_row_count);
      ATOMIC_AAF(&result_info_->deleted_, duplicate_row_count);
      row = rows.at(duplicate_row_count);
    } else if (ObLoadDupActionType::LOAD_IGNORE == dup_action_) {
      ATOMIC_AAF(&result_info_->skipped_, duplicate_row_count);
      row = rows.at(0);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected dup action", KR(ret), K_(dup_action));
    }
  }
  return ret;
}

int ObTableLoadDataRowInsertHandler::handle_update_row(
  ObArray<const ObDirectLoadMultipleDatumRow *> &rows,
  const ObDirectLoadMultipleDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(rows.count() < 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret));
  } else {
    const int64_t duplicate_row_count = rows.count() - 1;
    struct
    {
      bool operator()(const ObDirectLoadMultipleDatumRow *lhs, const ObDirectLoadMultipleDatumRow *rhs)
      {
        return lhs->seq_no_ < rhs->seq_no_;
      }
    } external_row_compare;
    lib::ob_sort(rows.begin(), rows.end(), external_row_compare);
    if (ObLoadDupActionType::LOAD_STOP_ON_DUP == dup_action_) {
      if (OB_FAIL(error_row_handler_->handle_error_row(OB_ERR_PRIMARY_KEY_DUPLICATE,
                                                       duplicate_row_count))) {
        LOG_WARN("fail to handle error row", KR(ret));
      } else {
        row = rows.at(0);
      }
    } else if (ObLoadDupActionType::LOAD_REPLACE == dup_action_) {
      ATOMIC_AAF(&result_info_->rows_affected_, 2 * duplicate_row_count);
      ATOMIC_AAF(&result_info_->deleted_, duplicate_row_count);
      row = rows.at(duplicate_row_count);
    } else if (ObLoadDupActionType::LOAD_IGNORE == dup_action_) {
      ATOMIC_AAF(&result_info_->skipped_, duplicate_row_count);
      row = rows.at(0);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected dup action", KR(ret), K_(dup_action));
    }
  }
  return ret;
}

int ObTableLoadDataRowInsertHandler::handle_update_row(const ObTabletID &tablet_id,
                                                       const ObDirectLoadDatumRow &old_row,
                                                       const ObDirectLoadDatumRow &new_row,
                                                       const ObDirectLoadDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataRowInsertHandler not init", KR(ret), KP(this));
  } else {
    if (ObLoadDupActionType::LOAD_STOP_ON_DUP == dup_action_) {
      if (OB_FAIL(error_row_handler_->handle_error_row(OB_ERR_PRIMARY_KEY_DUPLICATE))) {
        LOG_WARN("fail to handle error row", KR(ret));
      } else {
        result_row = &old_row;
      }
    } else if (ObLoadDupActionType::LOAD_IGNORE == dup_action_) {
      result_row = &old_row;
      ATOMIC_INC(&result_info_->skipped_);
    } else if (ObLoadDupActionType::LOAD_REPLACE == dup_action_) {
      result_row = &new_row;
      ATOMIC_INC(&result_info_->deleted_);
      ATOMIC_AAF(&result_info_->rows_affected_, 2);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected dup action", KR(ret), K_(dup_action));
    }
    if (OB_SUCC(ret) && result_row == &new_row) {
      // 发生replace
      if (ObDirectLoadMethod::is_incremental(store_ctx_->ctx_->param_.method_)) {
        // 删除旧行的lob
        if (nullptr != store_ctx_->data_store_table_ctx_->lob_table_ctx_) {
          ObTableLoadStoreLobTableCtx *lob_table_ctx =
            store_ctx_->data_store_table_ctx_->lob_table_ctx_;
          ObTableLoadLobTableBuilder *lob_builder = nullptr;
          if (OB_FAIL(lob_table_ctx->get_delete_table_builder(lob_builder))) {
            LOG_WARN("fail to get lob table builder", KR(ret));
          } else if (OB_FAIL(lob_builder->append_delete_row(tablet_id, old_row))) {
            LOG_WARN("fail to append delete row", KR(ret), K(tablet_id), K(old_row));
          }
        }
        // 删除旧行的索引, 插入新行的索引
        for (int64_t i = 0; OB_SUCC(ret) && i < store_ctx_->index_store_table_ctxs_.count(); ++i) {
          ObTableLoadStoreIndexTableCtx *index_table_ctx =
            store_ctx_->index_store_table_ctxs_.at(i);
          ObTableLoadIndexTableBuilder *index_builder = nullptr;
          if (OB_FAIL(index_table_ctx->get_insert_table_builder(index_builder))) {
            LOG_WARN("fail to get index table builder", KR(ret));
          } else if (OB_FAIL(index_builder->append_replace_row(tablet_id, old_row, new_row))) {
            LOG_WARN("fail to append replace row", KR(ret), K(tablet_id), K(old_row), K(new_row));
          }
        }
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase