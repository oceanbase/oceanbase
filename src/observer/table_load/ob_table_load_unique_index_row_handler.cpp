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

#include "observer/table_load/ob_table_load_unique_index_row_handler.h"
#include "observer/table_load/ob_table_load_data_table_builder.h"
#include "observer/table_load/ob_table_load_error_row_handler.h"
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
using namespace sql;
using namespace storage;

ObTableLoadUniqueIndexRowHandler::ObTableLoadUniqueIndexRowHandler()
  : store_ctx_(nullptr),
    error_row_handler_(nullptr),
    result_info_(nullptr),
    dup_action_(ObLoadDupActionType::LOAD_INVALID_MODE),
    is_inited_(false)
{
}

ObTableLoadUniqueIndexRowHandler::~ObTableLoadUniqueIndexRowHandler() {}

int ObTableLoadUniqueIndexRowHandler::init(ObTableLoadStoreCtx *store_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    int ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadUniqueIndexRowHandler init twice", KR(ret), KP(this));
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

int ObTableLoadUniqueIndexRowHandler::handle_insert_row(const ObTabletID &tablet_id,
                                                        const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (datum_row.is_ack_) {
    ObTableLoadDataTableBuilder *data_builder = nullptr;
    if (OB_FAIL(store_ctx_->data_store_table_ctx_->get_ack_table_builder(data_builder))) {
      LOG_WARN("fail to get ack table builder", KR(ret));
    } else if (OB_FAIL(data_builder->append_ack_row(tablet_id, datum_row))) {
      LOG_WARN("fail to append ack row", KR(ret), K(tablet_id), K(datum_row));
    }
  }
  return ret;
}

int ObTableLoadUniqueIndexRowHandler::handle_update_row(const ObTabletID &tablet_id,
                                                        const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (ObLoadDupActionType::LOAD_STOP_ON_DUP == dup_action_) {
      if (OB_FAIL(error_row_handler_->handle_error_row(OB_ERR_PRIMARY_KEY_DUPLICATE))) {
        LOG_WARN("fail to handle error row", KR(ret));
      } else {
        ATOMIC_DEC(&result_info_->rows_affected_); // delete one row in load data
        // 需要删除数据行
        ObTableLoadDataTableBuilder *data_builder = nullptr;
        if (OB_FAIL(store_ctx_->data_store_table_ctx_->get_delete_table_builder(data_builder))) {
          LOG_WARN("fail to get delete table builder", KR(ret));
        } else if (OB_FAIL(data_builder->append_delete_row(tablet_id, datum_row))) {
          LOG_WARN("fail to append delete row", KR(ret), K(tablet_id), K(datum_row));
        }
      }
    } else if (ObLoadDupActionType::LOAD_REPLACE == dup_action_) {
      ATOMIC_DEC(&result_info_->rows_affected_); // delete one row in data tablex
      // 需要删除数据行
      ObTableLoadDataTableBuilder *data_builder = nullptr;
      if (OB_FAIL(store_ctx_->data_store_table_ctx_->get_delete_table_builder(data_builder))) {
        LOG_WARN("fail to get delete table builder", KR(ret));
      } else if (OB_FAIL(data_builder->append_delete_row(tablet_id, datum_row))) {
        LOG_WARN("fail to append delete row", KR(ret), K(tablet_id), K(datum_row));
      }
    } else if (ObLoadDupActionType::LOAD_IGNORE == dup_action_) {
      ATOMIC_INC(&result_info_->skipped_);
      ATOMIC_DEC(&result_info_->rows_affected_); // delete one row in load data
      // 需要删除数据行
      ObTableLoadDataTableBuilder *data_builder = nullptr;
      if (OB_FAIL(store_ctx_->data_store_table_ctx_->get_delete_table_builder(data_builder))) {
        LOG_WARN("fail to get delete table builder", KR(ret));
      } else if (OB_FAIL(data_builder->append_delete_row(tablet_id, datum_row))) {
        LOG_WARN("fail to append delete row", KR(ret), K(tablet_id), K(datum_row));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected dup action", KR(ret), K_(dup_action));
    }
  }
  return ret;
}

int ObTableLoadUniqueIndexRowHandler::handle_update_row(
  const ObTabletID &tablet_id,
  ObArray<const ObDirectLoadExternalRow *> &rows,
  const ObDirectLoadExternalRow *&result_row)
{
  int ret = OB_SUCCESS;
  result_row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(rows.count() < 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret));
  } else {
    struct
    {
      bool operator()(const ObDirectLoadExternalRow *lhs, const ObDirectLoadExternalRow *rhs)
      {
        return lhs->seq_no_ < rhs->seq_no_;
      }
    } external_row_compare;
    const int64_t duplicate_row_count = rows.count() - 1;
    lib::ob_sort(rows.begin(), rows.end(), external_row_compare);
    if (ObLoadDupActionType::LOAD_STOP_ON_DUP == dup_action_) {
      if (OB_FAIL(error_row_handler_->handle_error_row(OB_ERR_PRIMARY_KEY_DUPLICATE,
                                                       duplicate_row_count))) {
        LOG_WARN("fail to handle error row", KR(ret));
      } else {
        result_row = rows.at(0);
        ATOMIC_FAS(&result_info_->rows_affected_, duplicate_row_count);
      }
    } else if (ObLoadDupActionType::LOAD_REPLACE == dup_action_) {
      ATOMIC_AAF(&result_info_->rows_affected_, duplicate_row_count);
      ATOMIC_AAF(&result_info_->deleted_, duplicate_row_count);
      result_row = rows.at(duplicate_row_count);
    } else if (ObLoadDupActionType::LOAD_IGNORE == dup_action_) {
      ATOMIC_AAF(&result_info_->skipped_, duplicate_row_count);
      ATOMIC_FAS(&result_info_->rows_affected_, duplicate_row_count);
      result_row = rows.at(0);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected dup action", KR(ret), K_(dup_action));
    }
    // mark result row is ack
    const_cast<ObDirectLoadExternalRow *>(result_row)->is_ack_ = true;
    // 删除其他行
    for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); i++) {
      const ObDirectLoadExternalRow *row = rows.at(i);
      ObTableLoadDataTableBuilder *data_builder = nullptr;
      if (row == result_row) {
      } else if (OB_FAIL(store_ctx_->data_store_table_ctx_->get_delete_table_builder(data_builder))) {
        LOG_WARN("fail to get delete table builder", KR(ret));
      } else if (OB_FAIL(data_builder->append_delete_row(tablet_id, *row))) {
        LOG_WARN("fail to append delete row", KR(ret), KPC(row));
      }
    }
  }
  return ret;
}

int ObTableLoadUniqueIndexRowHandler::handle_update_row(
  ObArray<const ObDirectLoadMultipleDatumRow *> &rows,
  const ObDirectLoadMultipleDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  result_row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(rows.count() < 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret));
  } else {
    struct
    {
      bool operator()(const ObDirectLoadMultipleDatumRow *lhs,
                      const ObDirectLoadMultipleDatumRow *rhs)
      {
        return lhs->seq_no_ < rhs->seq_no_;
      }
    } multiple_external_row_compare;
    const int64_t duplicate_row_count = rows.count() - 1;
    lib::ob_sort(rows.begin(), rows.end(), multiple_external_row_compare);
    if (ObLoadDupActionType::LOAD_STOP_ON_DUP == dup_action_) {
      if (OB_FAIL(error_row_handler_->handle_error_row(OB_ERR_PRIMARY_KEY_DUPLICATE,
                                                       duplicate_row_count))) {
        LOG_WARN("fail to handle error row", KR(ret));
      } else {
        result_row = rows.at(0);
        ATOMIC_FAS(&result_info_->rows_affected_, duplicate_row_count);
      }
    } else if (ObLoadDupActionType::LOAD_REPLACE == dup_action_) {
      ATOMIC_AAF(&result_info_->rows_affected_, duplicate_row_count);
      ATOMIC_AAF(&result_info_->deleted_, duplicate_row_count);
      result_row = rows.at(duplicate_row_count);
    } else if (ObLoadDupActionType::LOAD_IGNORE == dup_action_) {
      ATOMIC_AAF(&result_info_->skipped_, duplicate_row_count);
      ATOMIC_FAS(&result_info_->rows_affected_, duplicate_row_count);
      result_row = rows.at(0);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected dup action", KR(ret), K_(dup_action));
    }
    // mark result row is ack
    const_cast<ObDirectLoadMultipleDatumRow *>(result_row)->is_ack_ = true;
    // 删除其他行
    for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); i++) {
      const ObDirectLoadMultipleDatumRow *row = rows.at(i);
      ObTableLoadDataTableBuilder *data_builder = nullptr;
      if (row == result_row) {
      } else if (OB_FAIL(store_ctx_->data_store_table_ctx_->get_delete_table_builder(data_builder))) {
        LOG_WARN("fail to get delete table builder", KR(ret));
      } else if (OB_FAIL(data_builder->append_delete_row(*row))) {
        LOG_WARN("fail to append delete row", KR(ret), KPC(row));
      }
    }
  }
  return ret;
}

int ObTableLoadUniqueIndexRowHandler::handle_update_row(const ObTabletID &tablet_id,
                                                        const ObDirectLoadDatumRow &old_row,
                                                        const ObDirectLoadDatumRow &new_row,
                                                        const ObDirectLoadDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadRowHandler not init", KR(ret), KP(this));
  } else {
    if (ObLoadDupActionType::LOAD_STOP_ON_DUP == dup_action_) {
      if (OB_FAIL(error_row_handler_->handle_error_row(OB_ERR_PRIMARY_KEY_DUPLICATE))) {
        LOG_WARN("fail to handle error row", KR(ret));
      } else {
        result_row = &old_row;
        ATOMIC_DEC(&result_info_->rows_affected_); // delete one row in load data
      }
    } else if (ObLoadDupActionType::LOAD_IGNORE == dup_action_) {
      result_row = &old_row;
      ATOMIC_INC(&result_info_->skipped_);
      ATOMIC_DEC(&result_info_->rows_affected_); // delete one row in load data
    } else if (ObLoadDupActionType::LOAD_REPLACE == dup_action_) {
      result_row = &new_row;
      ATOMIC_INC(&result_info_->deleted_);
      ATOMIC_INC(&result_info_->rows_affected_); // delete one row in data table
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected dup action", KR(ret), K_(dup_action));
    }
    if (OB_SUCC(ret)) {
      ObTableLoadDataTableBuilder *delete_builder = nullptr;
      ObTableLoadDataTableBuilder *ack_builder = nullptr;
      if (OB_FAIL(store_ctx_->data_store_table_ctx_->get_delete_table_builder(delete_builder))) {
        LOG_WARN("fail to get delete table builder", KR(ret));
      } else if (OB_FAIL(store_ctx_->data_store_table_ctx_->get_ack_table_builder(ack_builder))) {
        LOG_WARN("fail to get ack table builder", KR(ret));
      } else {
        if (result_row == &old_row) {
          if (OB_FAIL(ack_builder->append_ack_row(tablet_id, old_row))) {
            LOG_WARN("fail to append ack row", KR(ret), K(old_row));
          } else if (OB_FAIL(delete_builder->append_delete_row(tablet_id, new_row))) {
            LOG_WARN("fail to append delete row", KR(ret), K(new_row));
          }
        } else {
          if (OB_FAIL(delete_builder->append_delete_row(tablet_id, old_row))) {
            LOG_WARN("fail to append delete row", KR(ret), K(old_row));
          } else if (OB_FAIL(ack_builder->append_ack_row(tablet_id, new_row))) {
            LOG_WARN("fail to append ack row", KR(ret), K(new_row));
          }
        }
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
