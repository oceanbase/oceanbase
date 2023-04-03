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
 *
 * SimpleBuf
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_buf.h"
#include "ob_log_utils.h"       // ob_cdc_malloc, ob_cdc_free

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{
SimpleBuf::SimpleBuf() :
    data_buf_(),
    use_data_buf_(true),
    big_buf_(NULL),
    buf_len_(0)
{
}

SimpleBuf::~SimpleBuf()
{
  destroy();
}

int SimpleBuf::init(const int64_t size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(size <= 0)) {
    LOG_ERROR("invalid argument", K(size));
    ret = OB_INVALID_ARGUMENT;
  } else {
    void *ptr = ob_cdc_malloc(size, "CDCSimpleBuf");

    if (OB_ISNULL(ptr)) {
      LOG_ERROR("ptr is NULL");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      data_buf_.set_data(static_cast<char *>(ptr), size);
      use_data_buf_ = true;
      buf_len_ = 0;
    }
  }

  return ret;
}

void SimpleBuf::destroy()
{
  char *data = data_buf_.get_data();
  if (NULL != data) {
    ob_cdc_free(data);
    data_buf_.reset();
  }

  if (NULL != big_buf_) {
    ob_cdc_free(big_buf_);
    big_buf_ = NULL;
  }

  use_data_buf_ = true;
  buf_len_ = 0;
}

int SimpleBuf::alloc(const int64_t sz, void *&ptr)
{
  int ret = OB_SUCCESS;
  ptr = data_buf_.alloc(sz);
  buf_len_ = sz;

  if (NULL != ptr) {
    use_data_buf_ = true;
  } else {
    if (OB_ISNULL(big_buf_ = static_cast<char *>(ob_cdc_malloc(sz, "CDCSimpleBuf")))) {
      LOG_ERROR("alloc big_buf_ fail");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      use_data_buf_ = false;
      ptr = big_buf_;
    }
  }

  return ret;
}

void SimpleBuf::free()
{
  if (use_data_buf_) {
    data_buf_.free();
  } else {
    if (NULL != big_buf_) {
      ob_cdc_free(big_buf_);
      big_buf_ = NULL;
    }
  }
}

const char *SimpleBuf::get_buf() const
{
  const char *ret_buf = NULL;

  if (use_data_buf_) {
    ret_buf = data_buf_.get_data();
  } else {
    ret_buf = big_buf_;
  }

  return ret_buf;
}

} // namespace libobcdc
} // namespace oceanbase
