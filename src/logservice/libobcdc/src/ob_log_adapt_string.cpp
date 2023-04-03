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

#define USING_LOG_PREFIX OBLOG

#include "ob_log_adapt_string.h"

#include "lib/allocator/ob_malloc.h"        // ob_free
#include "lib/utility/ob_print_utils.h"     // databuff_printf

using namespace oceanbase::common;
namespace oceanbase
{
namespace libobcdc
{

ObLogAdaptString::ObLogAdaptString(const char *label) :
    attr_(500, label),
    buf_()
{}

ObLogAdaptString::~ObLogAdaptString()
{
  if (NULL != buf_.get_data()) {
    ob_free(buf_.get_data());
  }
  buf_.reset();
}

int ObLogAdaptString::append(const char *data)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(data)) {
    LOG_ERROR("invalid argument", K(data));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t size = strlen(data);
    char *data_buf = NULL;

    // Prepare memory even if the data length is 0, because the empty string case should be supported
    if (OB_FAIL(alloc_buf_(size, data_buf))) {
      LOG_ERROR("allocate buffer fail", KR(ret), K(size), K(data_buf));
    }
    // Non-empty string case requires buffer to be valid
    else if (OB_UNLIKELY(size > 0 && NULL == data_buf)) {
      LOG_ERROR("data buffer is invalid", K(data_buf), K(size));
      ret = OB_ERR_UNEXPECTED;
    } else if (size > 0) {
      // copy data into data_buf
      (void)MEMCPY(data_buf, data, size);
    }
  }
  return ret;
}

int ObLogAdaptString::append_int64(const int64_t int_val)
{
  int ret = OB_SUCCESS;
  static const int64_t MAX_INT_CHAR_LEN = 32;
  char data_buf[MAX_INT_CHAR_LEN];
  // First print the number to the buffer, then append to the end
  if (OB_FAIL(databuff_printf(data_buf, sizeof(data_buf), "%ld", int_val))) {
    LOG_ERROR("databuff_printf fail", KR(ret), K(sizeof(data_buf)), K(data_buf), K(int_val));
  } else if (OB_FAIL(append(data_buf))) {
    LOG_ERROR("append string fail", KR(ret), K(sizeof(data_buf)), K(int_val));
  } else {
    // success
  }
  return ret;
}

// Supports calling the append function again after cstr
int ObLogAdaptString::cstr(const char *&str)
{
  int ret = OB_SUCCESS;
  if (buf_.get_data() == NULL) {
    str = "";
  }
  // Require that there must be space left to store \0
  else if (OB_UNLIKELY(buf_.get_remain() <= 0)) {
    LOG_ERROR("remain buffer is not enough", K(buf_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // Fill \0, but do not change the pos position, the purpose is to support continued filling
    buf_.get_data()[buf_.get_position()] = '\0';
    str = buf_.get_data();
  }
  return ret;
}

int ObLogAdaptString::alloc_buf_(const int64_t data_size, char *&data_buf)
{
  static const int64_t STRING_DEFAULT_SIZE = 8 * _K_;

  int ret = OB_SUCCESS;
  // The prepared buffer should always be larger than the data length, as it will be filled with \0 at the end.
  int64_t expected_buf_size = data_size + 1;
  data_buf = NULL;

  // First prepare the buffer, if the buffer is empty, then create a new buffer
  if (NULL == buf_.get_data()) {
    int64_t alloc_size = std::max(expected_buf_size, STRING_DEFAULT_SIZE);
    char *new_buf = static_cast<char *>(ob_malloc(alloc_size, attr_));

    if (OB_ISNULL(new_buf)) {
      LOG_ERROR("allocate memory fail", K(new_buf), K(alloc_size), K(expected_buf_size));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_UNLIKELY(! buf_.set_data(new_buf, alloc_size))) {
      LOG_ERROR("set data fail", K(buf_), K(new_buf), K(alloc_size));
      ret = OB_ERR_UNEXPECTED;
    }
  }
  // If there is not enough space left in the buffer, reallocate a larger space
  else if (buf_.get_remain() < expected_buf_size) {
    int64_t realloc_size = buf_.get_capacity() + std::max(expected_buf_size, STRING_DEFAULT_SIZE);
    char *new_buf = static_cast<char *>(ob_realloc(buf_.get_data(), realloc_size, attr_));

    if (OB_ISNULL(new_buf)) {
      LOG_ERROR("realloc memory fail", K(new_buf), K(realloc_size), K(expected_buf_size));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      int64_t pos = buf_.get_position();

      buf_.reset();
      if (OB_UNLIKELY(! buf_.set_data(new_buf, realloc_size))) {
        LOG_ERROR("set data fail", K(buf_), K(new_buf), K(realloc_size));
        ret = OB_ERR_UNEXPECTED;
      }
      // Reallocate previously allocated memory
      else if (OB_ISNULL(buf_.alloc(pos))) {
        LOG_ERROR("allocate old memory from buf fail", K(pos), K(buf_));
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }


  if (OB_SUCCESS == ret) {
    // After the buffer is ready, allocate the memory, allocate the memory of the size of the data, here you can not allocate \0 memory, because it will repeatedly fill the data
    // Allocate memory only if data_size is greater than 0
    if (data_size > 0) {
      if (OB_ISNULL(data_buf = static_cast<char *>(buf_.alloc(data_size)))) {
        LOG_ERROR("allocate buffer fail", KR(ret), K(data_size), K(buf_));
        ret = OB_ERR_UNEXPECTED;
      } else {
        // success
      }
    }
  }
  return ret;
}

}
}
