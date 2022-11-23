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
 * SimpleBuf, FixedBuf
 */

#ifndef OCEANBASE_LIBOBCDC_BUF_H_
#define OCEANBASE_LIBOBCDC_BUF_H_

#include "common/data_buffer.h"

namespace oceanbase
{
namespace libobcdc
{
template <int64_t BUFSIZE>
class FixedBuf
{
public:
  FixedBuf() { reset(); }
  ~FixedBuf() { reset(); }
  void reset()
  {
    buf_[0] = '\0';
    pos_ = 0;
  }
  void destroy() { reset(); }

  char *get_buf() { return buf_; }
  int64_t get_size() const { return BUFSIZE; }
  int64_t get_len() const { return pos_; }
  int fill(const char *data, const int64_t data_len)
  {
    int ret = common::OB_SUCCESS;

    if (OB_ISNULL(data) || OB_UNLIKELY(data_len <= 0)) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if (OB_UNLIKELY(pos_ + data_len >= BUFSIZE)) {
      ret = common::OB_BUF_NOT_ENOUGH;
    } else {
      MEMCPY(buf_ + pos_, data, data_len);
      pos_ += data_len;
      buf_[pos_] = '\0';
    }

    return ret;
  }

public:
  static const int64_t RP_MAX_FREE_LIST_NUM = 1024;

private:
  char buf_[BUFSIZE];
  int64_t pos_;
};

struct SimpleBuf
{
  SimpleBuf();
  ~SimpleBuf();

  int init(const int64_t size);
  void destroy();

  int alloc(const int64_t sz, void *&ptr);
  void free();

  int64_t get_buf_len() const { return buf_len_; }
  const char *get_buf() const;

  common::ObDataBuffer data_buf_;
  bool use_data_buf_;
  char *big_buf_;
  int64_t buf_len_;
};


} // namespace libobcdc
} // namespace oceanbase
#endif
