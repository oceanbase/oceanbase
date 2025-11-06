/**
 * Copyright (c) 2025 OceanBase
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

#include "observer/table_load/plan/ob_table_load_data_table_row_handler.h"
#include "observer/table_load/ob_table_load_error_row_handler.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/plan/ob_table_load_plan.h"
#include "observer/table_load/plan/ob_table_load_table_op.h"
#include "storage/direct_load/ob_direct_load_external_row.h"
#include "storage/direct_load/ob_direct_load_multiple_datum_row.h"

namespace oceanbase
{
namespace observer
{
using namespace blocksstable;
using namespace common;
using namespace sql;
using namespace storage;

/**
 * ObTableLoadDataTableInsertRowHandler
 */

ObTableLoadDataTableInsertRowHandler::ObTableLoadDataTableInsertRowHandler(
  ObTableLoadTableOp *table_op)
  : ObTableLoadTableDMLRowHandler(table_op),
    error_row_handler_(nullptr),
    result_info_(nullptr),
    dup_action_(ObLoadDupActionType::LOAD_INVALID_MODE)
{
}

int ObTableLoadDataTableInsertRowHandler::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    int ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadDataTableInsertRowHandler init twice", KR(ret), KP(this));
  } else {
    ObTableLoadStoreCtx *store_ctx = table_op_->get_plan()->get_store_ctx();
    error_row_handler_ = store_ctx->error_row_handler_;
    result_info_ = &store_ctx->result_info_;
    dup_action_ = store_ctx->ctx_->param_.dup_action_;
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadDataTableInsertRowHandler::handle_insert_row(const ObTabletID &tablet_id,
                                                            const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataTableInsertRowHandler not init", KR(ret), KP(this));
  } else {
    ATOMIC_INC(&result_info_->rows_affected_);
    if (OB_FAIL(push_insert_row(tablet_id, datum_row))) {
      LOG_WARN("fail to push insert row", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadDataTableInsertRowHandler::handle_insert_row(const ObTabletID &tablet_id,
                                                            const ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataTableInsertRowHandler not init", KR(ret), KP(this));
  } else {
    ATOMIC_INC(&result_info_->rows_affected_);
    if (OB_FAIL(push_insert_row(tablet_id, datum_row))) {
      LOG_WARN("fail to push insert row", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadDataTableInsertRowHandler::handle_insert_batch(const ObTabletID &tablet_id,
                                                              const ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataTableInsertRowHandler not init", KR(ret), KP(this));
  } else if (0 == datum_rows.row_count_) {
    // do nothing
  } else {
    ATOMIC_AAF(&result_info_->rows_affected_, datum_rows.row_count_);
    if (OB_FAIL(push_insert_batch(tablet_id, datum_rows))) {
      LOG_WARN("fail to push insert batch", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadDataTableInsertRowHandler::handle_update_row(const ObTabletID &tablet_id,
                                                            const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (ObLoadDupActionType::LOAD_STOP_ON_DUP == dup_action_) {
      LOG_INFO("duplicate row", K(tablet_id), K(datum_row));
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
    if (OB_SUCC(ret)) {
      if (OB_FAIL(push_update_row(tablet_id, datum_row))) {
        LOG_WARN("fail to push update row", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadDataTableInsertRowHandler::handle_update_row(
  const ObTabletID &tablet_id, ObArray<const ObDirectLoadExternalRow *> &rows,
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
    const int64_t duplicate_row_count = rows.count() - 1;
    struct
    {
      bool operator()(const ObDirectLoadExternalRow *lhs, const ObDirectLoadExternalRow *rhs)
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
    if (OB_SUCC(ret)) {
      if (OB_FAIL(push_update_row(tablet_id, rows, row))) {
        LOG_WARN("fail to push update row", K(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadDataTableInsertRowHandler::handle_update_row(
  ObArray<const ObDirectLoadMultipleDatumRow *> &rows, const ObDirectLoadMultipleDatumRow *&row)
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
      bool operator()(const ObDirectLoadMultipleDatumRow *lhs,
                      const ObDirectLoadMultipleDatumRow *rhs)
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
    if (OB_SUCC(ret)) {
      if (OB_FAIL(push_update_row(rows, row))) {
        LOG_WARN("fail to push update row", K(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadDataTableInsertRowHandler::handle_update_row(const ObTabletID &tablet_id,
                                                            const ObDirectLoadDatumRow &old_row,
                                                            const ObDirectLoadDatumRow &new_row,
                                                            const ObDirectLoadDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataTableInsertRowHandler not init", KR(ret), KP(this));
  } else {
    if (ObLoadDupActionType::LOAD_STOP_ON_DUP == dup_action_) {
      LOG_INFO("duplicate row", K(tablet_id), K(old_row), K(new_row));
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
    if (OB_SUCC(ret)) {
      if (OB_FAIL(push_update_row(tablet_id, old_row, new_row, result_row))) {
        LOG_WARN("fail to push update row", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadDataTableInsertRowHandler::handle_insert_delete_conflict(const ObTabletID &tablet_id,
                                                                         const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataTableInsertRowHandler not init", KR(ret), KP(this));
  } else {
    ATOMIC_INC(&result_info_->rows_affected_);
    if (OB_FAIL(push_insert_delete_conflict(tablet_id, datum_row))) {
      LOG_WARN("fail to push insert delete conflict", KR(ret));
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
