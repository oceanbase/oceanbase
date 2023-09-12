// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   yuya.yu <>

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_error_row_handler.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"

namespace oceanbase
{
namespace observer
{
using namespace blocksstable;
using namespace common;
using namespace sql;

ObTableLoadErrorRowHandler::ObTableLoadErrorRowHandler()
  : dup_action_(ObLoadDupActionType::LOAD_INVALID_MODE),
    max_error_row_count_(0),
    result_info_(nullptr),
    job_stat_(nullptr),
    error_row_count_(0),
    is_inited_(false)
{
}

ObTableLoadErrorRowHandler::~ObTableLoadErrorRowHandler()
{
}

int ObTableLoadErrorRowHandler::init(ObTableLoadStoreCtx *store_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadErrorRowHandler init twice", KR(ret), KP(this));
  } else {
    dup_action_ = store_ctx->ctx_->param_.dup_action_;
    max_error_row_count_ = store_ctx->ctx_->param_.max_error_row_count_;
    result_info_ = &store_ctx->result_info_;
    job_stat_ = store_ctx->ctx_->job_stat_;
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadErrorRowHandler::handle_insert_row(const blocksstable::ObDatumRow &row)
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ATOMIC_INC(&result_info_->rows_affected_);
  }
  return ret;
}

int ObTableLoadErrorRowHandler::handle_update_row(const ObDatumRow &row)
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (ObLoadDupActionType::LOAD_STOP_ON_DUP == dup_action_) {
      if (0 == max_error_row_count_) {
        ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
      } else {
        ObMutexGuard guard(mutex_);
        if (error_row_count_ >= max_error_row_count_) {
          ret = OB_ERR_TOO_MANY_ROWS;
          LOG_WARN("error row count reaches its maximum value", KR(ret), K_(max_error_row_count),
                   K_(error_row_count));
        } else {
          ++error_row_count_;
        }
      }
      ATOMIC_INC(&job_stat_->detected_error_rows_);
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

int ObTableLoadErrorRowHandler::handle_update_row(const ObDatumRow &old_row,
                                                  const ObDatumRow &new_row,
                                                  const ObDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  result_row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (ObLoadDupActionType::LOAD_STOP_ON_DUP == dup_action_) {
      if (0 == max_error_row_count_) {
        ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
      } else {
        ObMutexGuard guard(mutex_);
        if (error_row_count_ >= max_error_row_count_) {
          ret = OB_ERR_TOO_MANY_ROWS;
          LOG_WARN("error row count reaches its maximum value", KR(ret), K_(max_error_row_count),
                   K_(error_row_count));
        } else {
          ++error_row_count_;
        }
      }
      if (OB_SUCC(ret)) {
        result_row = &old_row;
      }
      ATOMIC_INC(&job_stat_->detected_error_rows_);
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
  }
  return ret;
}

int ObTableLoadErrorRowHandler::handle_error_row(int error_code, const ObNewRow &row)
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (max_error_row_count_ == 0) {
    ret = error_code;
  } else {
    ObMutexGuard guard(mutex_);
    if (error_row_count_ >= max_error_row_count_) {
      ret = OB_ERR_TOO_MANY_ROWS;
      LOG_WARN("error row count reaches its maximum value", KR(ret), K_(max_error_row_count),
               K_(error_row_count));
    } else {
      ++error_row_count_;
    }
    ATOMIC_INC(&job_stat_->detected_error_rows_);
  }
  return ret;
}

int ObTableLoadErrorRowHandler::handle_error_row(int error_code, const ObDatumRow &row)
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (max_error_row_count_ == 0) {
    ret = error_code;
  } else {
    ObMutexGuard guard(mutex_);
    if (error_row_count_ >= max_error_row_count_) {
      ret = OB_ERR_TOO_MANY_ROWS;
      LOG_WARN("error row count reaches its maximum value", KR(ret), K_(max_error_row_count),
               K_(error_row_count));
    } else {
      ++error_row_count_;
    }
    ATOMIC_INC(&job_stat_->detected_error_rows_);
  }
  return ret;
}

uint64_t ObTableLoadErrorRowHandler::get_error_row_count() const
{
  ObMutexGuard guard(mutex_);
  return error_row_count_;
}

} // namespace observer
} // namespace oceanbase
