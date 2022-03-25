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

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_entry_wrapper.h"

namespace oceanbase
{
using namespace common;
namespace liboblog
{
ObLogEntryWrapper::ObLogEntryWrapper(const bool is_pg,
    const clog::ObLogEntry &log_entry,
    ObLogAggreTransLog &aggre_trans_log)
    : is_pg_(is_pg),
      log_entry_(log_entry),
      aggre_trans_log_(aggre_trans_log)
{
}

ObLogEntryWrapper::~ObLogEntryWrapper()
{
  is_pg_ = false;
}

int64_t ObLogEntryWrapper::get_submit_timestamp() const
{
  int64_t submit_timestamp = OB_INVALID_TIMESTAMP;

  if (! is_pg_) {
    submit_timestamp = log_entry_.get_header().get_submit_timestamp();
  } else {
    submit_timestamp = aggre_trans_log_.submit_timestamp_;
  }

  return submit_timestamp;
}

const char *ObLogEntryWrapper::get_buf() const
{
  const char *res = NULL;

  if (! is_pg_) {
    res = log_entry_.get_buf();
  } else {
    res = aggre_trans_log_.buf_;
  }

  return res;
}

int64_t ObLogEntryWrapper::get_buf_len() const
{
  int64_t buf_len = 0;

  if (! is_pg_) {
    buf_len = log_entry_.get_header().get_data_len();
  } else {
    buf_len = aggre_trans_log_.buf_len_;
  }

  return buf_len;
}

bool ObLogEntryWrapper::is_batch_committed() const
{
  return log_entry_.is_batch_committed();
}

} // liboblog
} // oceanbase
