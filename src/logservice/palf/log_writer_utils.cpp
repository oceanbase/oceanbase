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

#include "log_writer_utils.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/serialization.h"
#include "logservice/palf/log_define.h"
namespace oceanbase
{
using namespace common;
namespace palf
{
LogWriteBuf::LogWriteBuf()
{
  reset();
}
LogWriteBuf::LogWriteBuf(const LogWriteBuf &write_buf)
{
  write_buf_ = write_buf.write_buf_;
}
LogWriteBuf::~LogWriteBuf()
{
  reset();
}
void LogWriteBuf::reset()
{
  write_buf_.reset();
}
bool LogWriteBuf::is_valid() const
{
  return 0 < write_buf_.count();
}

int LogWriteBuf::merge(const LogWriteBuf &rhs, bool &has_merged)
{
  int ret = OB_SUCCESS;
  has_merged = false;
  int64_t lhs_size = this->get_total_size();
  int64_t lhs_count = this->get_buf_count();
  int64_t rhs_size = rhs.get_total_size();
  int64_t rhs_count = rhs.get_buf_count();
  if (2 <= lhs_count || 2 <= rhs_count) {
    PALF_LOG(INFO, "no need to merge", K(ret), K(lhs_count), K(rhs_count));
  } else if (MAX_LOG_BUFFER_SIZE < lhs_size || MAX_LOG_BUFFER_SIZE < rhs_size) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "the size if greater than MAX_LOG_BUFFER_SIZE, unexpected error!!!",
        K(ret), K(lhs_size), K(rhs_size), K(lhs_count), K(rhs_count));
  } else if (MAX_LOG_BUFFER_SIZE < lhs_size + rhs_size) {
    PALF_LOG(INFO, "the size is overflow", K(ret), K(lhs_size), K(rhs_size), KPC(this), K(rhs));
  } else if (write_buf_[0].buf_ == rhs.write_buf_[0].buf_) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "the pointer is same, unexpected error", K(ret), KP(write_buf_[0].buf_),
        KP(rhs.write_buf_[0].buf_));
  } else if (write_buf_[0].buf_ + lhs_size != rhs.write_buf_[0].buf_) {
    //ret = OB_ERR_UNEXPECTED;
    PALF_LOG(INFO, "write_buf is not continous, no need merge", K(ret), KPC(this), K(rhs));
  } else {
    write_buf_[0].buf_len_ += rhs_size;
    has_merged = true;
    PALF_LOG(TRACE, "merge success", KPC(this), K(rhs), K(has_merged));
  }
  return ret;
}

int LogWriteBuf::push_back(const char *buf,
                           const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || 0 >= buf_len) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), KP(buf), K(buf_len));
  } else {
    int64_t remain_buf_len = buf_len;
    int64_t step = MAX_LOG_BUFFER_SIZE;
    while (OB_SUCC(ret) && remain_buf_len > 0) {
      const int64_t limit_buf_len = MIN(remain_buf_len, step);
      InnerStruct inner_struct;
      inner_struct.buf_ = (buf + buf_len - remain_buf_len);
      inner_struct.buf_len_ = limit_buf_len;
      remain_buf_len -= limit_buf_len;
      ret = write_buf_.push_back(inner_struct);
    }
  }
  return ret;
}
int LogWriteBuf::get_write_buf(const int64_t idx,
                               const char *&buf,
                               int64_t &buf_len) const
{
  int ret = OB_SUCCESS;
  if (idx >= write_buf_.count()) {
    ret = OB_ARRAY_OUT_OF_RANGE;
  } else {
    buf = write_buf_[idx].buf_;
    buf_len = write_buf_[idx].buf_len_;
  }
  return ret;
}
int64_t LogWriteBuf::get_total_size() const
{
  int64_t total_size = 0;
  for (int64_t i = 0; i < write_buf_.count(); i++) {
    total_size += write_buf_[i].buf_len_;
  }
  return total_size;
}

int64_t LogWriteBuf::get_buf_count() const
{
  return write_buf_.count();
}

bool LogWriteBuf::check_memory_is_continous() const
{
  bool bool_ret = true;
  for (int64_t i = 0; i < write_buf_.count()-1; i++) {
    if (write_buf_[i].buf_ + write_buf_[i].buf_len_ != write_buf_[i+1].buf_) {
      bool_ret = false;
      break;
    }
  }
  return bool_ret;
}

void LogWriteBuf::memcpy_to_continous_memory(char *dest_buf) const
{
  int64_t pos = 0;
  for (int64_t i = 0; i < write_buf_.count(); i++) {
    MEMCPY(dest_buf+pos, write_buf_[i].buf_, write_buf_[i].buf_len_);
    pos += write_buf_[i].buf_len_;
  }
}

DEFINE_SERIALIZE(LogWriteBuf)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  const int64_t total_size = get_total_size();
  if (NULL == buf || 0 >= buf_len || 0 > pos) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, total_size))) {
    PALF_LOG(ERROR, "LogWriteBuf serialize failed", K(ret), K(pos), K(new_pos), K(buf_len));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < write_buf_.count(); i++) {
      const int64_t tmp_buf_len = write_buf_[i].buf_len_;
      MEMCPY(buf+new_pos, write_buf_[i].buf_, tmp_buf_len);
      new_pos += tmp_buf_len;
    }
  }
  if (OB_SUCC(ret)) {
    pos = new_pos;
  }
  return ret;
}
DEFINE_DESERIALIZE(LogWriteBuf)
{
  int ret = OB_SUCCESS;
  int64_t total_size = 0;
  int64_t new_pos = pos;
  if (NULL == buf || 0 >= data_len || 0 > pos) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &total_size))) {
    PALF_LOG(ERROR, "LogWriteBuf deserialize failed", K(ret), K(pos), K(new_pos), K(data_len));
  } else {
    InnerStruct inner_struct;
    inner_struct.buf_ = buf + new_pos;
    inner_struct.buf_len_ = total_size;
    if (OB_FAIL(write_buf_.push_back(inner_struct))){
      PALF_LOG(ERROR, "LogWriteBuf push_back failed", K(ret));
    } else {
      pos = new_pos + total_size;
    }
  }
  return ret;
}
DEFINE_GET_SERIALIZE_SIZE(LogWriteBuf)
{
  int64_t size = 0;
  const int64_t total_size = this->get_total_size();
  size += serialization::encoded_length_i64(total_size);
  size += total_size;
  return size;
}
} // end namespace palf
} // end namespace oceanbase
