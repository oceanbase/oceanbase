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

#include "observer/table_load/ob_table_load_data_row_handler.h"
#include "observer/table_load/ob_table_load_error_row_handler.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_index_table_projector.h"
#include "storage/direct_load/ob_direct_load_i_table.h"

namespace oceanbase
{
namespace observer
{
ObTableLoadDataRowHandler::ObTableLoadDataRowHandler()
  : error_row_handler_(nullptr),
    index_store_table_ctxs_(nullptr),
    result_info_(nullptr),
    is_inited_(false)
{
}

ObTableLoadDataRowHandler::~ObTableLoadDataRowHandler()
{

}

int ObTableLoadDataRowHandler::init(const ObTableLoadParam &param,
                                    table::ObTableLoadResultInfo &result_info,
                                    ObTableLoadErrorRowHandler *error_row_handler,
                                    ObArray<ObTableLoadStoreTableCtx *> *index_store_table_ctxs)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    int ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadDataRowHandler init twice", KR(ret), KP(this));
  } else if (OB_ISNULL(error_row_handler) || OB_ISNULL(index_store_table_ctxs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", KR(ret), KP(error_row_handler), KP(index_store_table_ctxs));
  } else {
    error_row_handler_ = error_row_handler;
    index_store_table_ctxs_ = index_store_table_ctxs;
    result_info_ = &result_info;
    dup_action_ = param.dup_action_;
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadDataRowHandler::handle_insert_row(const ObTabletID tablet_id,
                                                 const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataRowHandler not init", KR(ret), KP(this));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_store_table_ctxs_->count(); i++) {
      ObTableLoadStoreTableCtx *store_table_ctx = index_store_table_ctxs_->at(i);
      ObTabletID index_tablet_id;
      blocksstable::ObDatumRow index_row;
      ObIDirectLoadPartitionTableBuilder *builder = nullptr;
      if (OB_FAIL(store_table_ctx->project_->projector(tablet_id, row, false, index_tablet_id,
                                                       index_row))) {
        LOG_WARN("fail to projector", KR(ret), K(tablet_id), K(row));
      } else {
        index_row.row_flag_.set_flag(DF_INSERT);
        if (OB_FAIL(store_table_ctx->get_index_table_builder(builder))) {
          LOG_WARN("get builder failed", KR(ret));
        } else if (OB_FAIL(builder->append_row(index_tablet_id, 0, index_row))) {
          LOG_WARN("add row failed", KR(ret), K(index_row));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ATOMIC_INC(&result_info_->rows_affected_);
    }
  }
  return ret;
}

int ObTableLoadDataRowHandler::handle_insert_batch(const ObTabletID &tablet_id, const ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataRowHandler not init", KR(ret), KP(this));
  } else if (0 == datum_rows.row_count_) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_store_table_ctxs_->count(); i++) {
      ObTableLoadStoreTableCtx *store_table_ctx = index_store_table_ctxs_->at(i);
      ObIDirectLoadPartitionTableBuilder *builder = nullptr;
      ObTabletID index_tablet_id;
      ObDatumRow index_datum_row;
      index_datum_row.row_flag_.set_flag(DF_INSERT);
      if (OB_FAIL(store_table_ctx->get_index_table_builder(builder))) {
        LOG_WARN("fail to get index table builder", KR(ret));
      } else if (OB_FAIL(
                   store_table_ctx->project_->get_index_tablet_id(tablet_id, index_tablet_id))) {
        LOG_WARN("fail to get index tablet id", KR(ret));
      } else if (OB_FAIL(store_table_ctx->project_->init_datum_row(index_datum_row))) {
        LOG_WARN("fail to projector", KR(ret), K(tablet_id));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < datum_rows.row_count_; ++i) {
        if (OB_FAIL(store_table_ctx->project_->projector(
              datum_rows, i, false /*has_multi_version_cols*/, index_datum_row))) {
          LOG_WARN("fail to projector", KR(ret), K(tablet_id));
        } else if (OB_FAIL(builder->append_row(index_tablet_id, 0, index_datum_row))) {
          LOG_WARN("add row failed", KR(ret), K(index_datum_row));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ATOMIC_AAF(&result_info_->rows_affected_, datum_rows.row_count_);
    }
  }
  return ret;
}

int ObTableLoadDataRowHandler::handle_insert_row_with_multi_version(const ObTabletID tablet_id, const blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataRowHandler not init", KR(ret), KP(this));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_store_table_ctxs_->count(); i++) {
      ObTableLoadStoreTableCtx *store_table_ctx = index_store_table_ctxs_->at(i);
      ObTabletID index_tablet_id;
      blocksstable::ObDatumRow index_row;
      ObIDirectLoadPartitionTableBuilder *builder = nullptr;
      if (OB_FAIL(store_table_ctx->project_->projector(tablet_id, row, true, index_tablet_id,
                                                       index_row))) {
        LOG_WARN("fail to projector", KR(ret), K(tablet_id), K(row));
      } else {
        index_row.row_flag_.set_flag(blocksstable::DF_INSERT);
        if (OB_FAIL(store_table_ctx->get_index_table_builder(builder))) {
          LOG_WARN("get builder failed", KR(ret));
        } else if (OB_FAIL(builder->append_row(index_tablet_id, 0, index_row))) {
          LOG_WARN("add row failed", KR(ret), K(index_row));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ATOMIC_INC(&result_info_->rows_affected_);
    }
  }
  return ret;
}

int ObTableLoadDataRowHandler::handle_insert_batch_with_multi_version(const ObTabletID &tablet_id, const ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataRowHandler not init", KR(ret), KP(this));
  } else if (0 == datum_rows.row_count_) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_store_table_ctxs_->count(); i++) {
      ObTableLoadStoreTableCtx *store_table_ctx = index_store_table_ctxs_->at(i);
      ObIDirectLoadPartitionTableBuilder *builder = nullptr;
      ObTabletID index_tablet_id;
      ObDatumRow index_datum_row;
      index_datum_row.row_flag_.set_flag(DF_INSERT);
      if (OB_FAIL(store_table_ctx->get_index_table_builder(builder))) {
        LOG_WARN("fail to get index table builder", KR(ret));
      } else if (OB_FAIL(
                   store_table_ctx->project_->get_index_tablet_id(tablet_id, index_tablet_id))) {
        LOG_WARN("fail to get index tablet id", KR(ret));
      } else if (OB_FAIL(store_table_ctx->project_->init_datum_row(index_datum_row))) {
        LOG_WARN("fail to projector", KR(ret), K(tablet_id));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < datum_rows.row_count_; ++i) {
        if (OB_FAIL(store_table_ctx->project_->projector(
              datum_rows, i, true /*has_multi_version_cols*/, index_datum_row))) {
          LOG_WARN("fail to projector", KR(ret), K(tablet_id));
        } else if (OB_FAIL(builder->append_row(index_tablet_id, 0, index_datum_row))) {
          LOG_WARN("add row failed", KR(ret), K(index_datum_row));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ATOMIC_AAF(&result_info_->rows_affected_, datum_rows.row_count_);
    }
  }
  return ret;
}

int ObTableLoadDataRowHandler::handle_update_row(const blocksstable::ObDatumRow &row)
{
  UNUSED(row);
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

int ObTableLoadDataRowHandler::handle_update_row(const ObTabletID tablet_id,
                                             const blocksstable::ObDatumRow &old_row,
                                             const blocksstable::ObDatumRow &new_row,
                                             const blocksstable::ObDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataRowHandler not init", KR(ret), KP(this));
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
      for (int64_t i = 0; OB_SUCC(ret) && i < index_store_table_ctxs_->count(); i++) {
        ObTableLoadStoreTableCtx *store_table_ctx = index_store_table_ctxs_->at(i);
        ObTabletID index_tablet_id;
        blocksstable::ObDatumRow index_row_old;
        blocksstable::ObDatumRow index_row_new;
        ObIDirectLoadPartitionTableBuilder *builder = nullptr;
        if (OB_FAIL(store_table_ctx->project_->projector(tablet_id, old_row, false, index_tablet_id,
                                                         index_row_old))) {
          LOG_WARN("fail to projector", KR(ret), K(tablet_id), K(old_row));
        } else if (OB_FAIL(store_table_ctx->project_->projector(tablet_id, new_row, false,
                                                                index_tablet_id, index_row_new))) {
          LOG_WARN("fail to projector", KR(ret), K(tablet_id), K(new_row));
        } else if (OB_FAIL(store_table_ctx->get_index_table_builder(builder))) {
          LOG_WARN("get builder failed", KR(ret));
        } else {
          ObDatumRowkey old_key(index_row_old.storage_datums_,
                                store_table_ctx->schema_->rowkey_column_count_);
          ObDatumRowkey new_key(index_row_new.storage_datums_,
                                store_table_ctx->schema_->rowkey_column_count_);
          int cmp_ret;
          if (OB_FAIL(old_key.compare(new_key, store_table_ctx->schema_->datum_utils_, cmp_ret))) {
            LOG_WARN("fail to compare", KR(ret));
          } else {
            // when cmp_ret == 0, insert new row is equal to  delete old row and insert new row.
            if (0 != cmp_ret) {
              index_row_old.row_flag_.set_flag(blocksstable::DF_DELETE,
                                               blocksstable::ObDmlRowFlagType::DF_TYPE_NORMAL);
              if (OB_FAIL(builder->append_row(index_tablet_id, 0, index_row_old))) {
                LOG_WARN("add row failed", KR(ret), K(index_row_old));
              }
            }
          }
          if (OB_SUCC(ret)) {
            index_row_new.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
            if (OB_FAIL(builder->append_row(index_tablet_id, 0, index_row_new))) {
              LOG_WARN("add row failed", KR(ret), K(index_row_new));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLoadDataRowHandler::handle_update_row(common::ObArray<const ObDirectLoadExternalRow *> &rows,
                                             const ObDirectLoadExternalRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(rows.count() < 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret));
  } else {
    int64_t duplicate_row_count = rows.count() - 1;
    lib::ob_sort(rows.begin(), rows.end(), ObTableLoadDataRowHandler::external_row_compare);
    if (ObLoadDupActionType::LOAD_STOP_ON_DUP == dup_action_) {
      if (OB_FAIL(error_row_handler_->handle_error_row(OB_ERR_PRIMARY_KEY_DUPLICATE, duplicate_row_count))) {
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

int ObTableLoadDataRowHandler::handle_update_row(
  common::ObArray<const ObDirectLoadMultipleDatumRow *> &rows,
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
    int64_t duplicate_row_count = rows.count() - 1;
    lib::ob_sort(rows.begin(), rows.end(), ObTableLoadDataRowHandler::multiple_external_row_compare);
    if (ObLoadDupActionType::LOAD_STOP_ON_DUP == dup_action_) {
      if (OB_FAIL(error_row_handler_->handle_error_row(OB_ERR_PRIMARY_KEY_DUPLICATE, duplicate_row_count))) {
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

} // namespace observer
} // namespace oceanbase