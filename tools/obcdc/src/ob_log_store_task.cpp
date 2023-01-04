/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_store_task.h"

namespace oceanbase
{
namespace liboblog
{
using namespace oceanbase::common;

ObLogStoreTask::ObLogStoreTask()
  : is_inited_(false),
    store_key_(),
    data_buf_(NULL),
    data_len_(0),
    offset_(0),
    log_callback_(NULL)
{
  need_callback_ = true;
}

ObLogStoreTask::~ObLogStoreTask()
{
  reset();
}

void ObLogStoreTask::reset()
{
  IObLogBufTask::reset();
  is_inited_ = false;
  store_key_.reset();
  data_buf_ = NULL;
  data_len_ = 0;
  offset_ = 0;
  log_callback_ = NULL;
  need_callback_ = true;
}

int ObLogStoreTask::init(const uint64_t tenant_id,
    const char *participant_key,
    const uint64_t log_id,
    const int32_t log_offset,
    const char *data_buf,
    const int64_t data_len,
    ObILogCallback *log_callback)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(log_callback) || OB_ISNULL(data_buf) || OB_UNLIKELY(data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(store_key_.init(tenant_id, participant_key, log_id, log_offset))) {
  } else {
    data_buf_ = data_buf;
    data_len_ = data_len;
    offset_ = 0;
    log_callback_ = log_callback;

    is_inited_ = true;
  }

  return ret;
}

bool ObLogStoreTask::is_valid() const
{
  return is_inited_ && store_key_.is_valid() && data_len_ > 0;
}

int ObLogStoreTask::fill_buffer(char *buf, const offset_t offset)
{
  int ret = OB_SUCCESS;

  // TODO is_valid_offset
  if (OB_ISNULL(buf) || OB_UNLIKELY(offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    offset_ = offset;
    MEMCPY(buf + offset, data_buf_, data_len_);
  }

  return ret;
}

int ObLogStoreTask::st_after_consume(const int handle_err)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_SUCCESS != handle_err) {
    LOG_ERROR("st_after_consume fail", K(handle_err));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(log_callback_->handle_log_callback())) {
    LOG_ERROR("handle_log_callback fail", KR(ret));
  } else {
    // succ
  }

  return ret;
}

}
}
