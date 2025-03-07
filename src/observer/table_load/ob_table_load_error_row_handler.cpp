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

#include "observer/table_load/ob_table_load_error_row_handler.h"

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

int ObTableLoadErrorRowHandler::init(const ObTableLoadParam &param,
                                     table::ObTableLoadResultInfo &result_info,
                                     sql::ObLoadDataStat *job_stat)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadErrorRowHandler init twice", KR(ret), KP(this));
  } else {
    dup_action_ = param.dup_action_;
    max_error_row_count_ = param.max_error_row_count_;
    result_info_ = &result_info;
    job_stat_ = job_stat;
    is_inited_ = true;
  }
  return ret;
}





int ObTableLoadErrorRowHandler::handle_error_row(int error_code)
{
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

int ObTableLoadErrorRowHandler::handle_error_row(int error_code, int64_t duplicate_row_count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (max_error_row_count_ == 0) {
    ret = error_code;
  } else {
    if (0 == max_error_row_count_) {
      ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
    } else {
      ObMutexGuard guard(mutex_);
      error_row_count_ += duplicate_row_count;
      if (error_row_count_ > max_error_row_count_) {
        ret = OB_ERR_TOO_MANY_ROWS;
        LOG_WARN("error row count reaches its maximum value", KR(ret), K_(max_error_row_count),
                 K_(error_row_count));
      }
    }
    ATOMIC_AAF(&job_stat_->detected_error_rows_, duplicate_row_count);
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
