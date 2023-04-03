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

#include "lsn.h"
#include "log_define.h"

namespace oceanbase {
namespace palf{
using namespace common;
LSN::LSN()
{
  reset();
}

LSN::LSN(const offset_t offset)
    : val_(offset)
{
}

LSN::LSN(const LSN &lsn) : val_(lsn.val_)
{
}

bool LSN::is_valid() const
{
  return LOG_INVALID_LSN_VAL != val_;
}

void LSN::reset()
{
  val_ = LOG_INVALID_LSN_VAL;
}

bool operator==(const uint64_t offset, const LSN &lsn)
{
  return offset == lsn.val_;
}

bool LSN::operator==(const LSN &lsn) const
{
  return val_ == lsn.val_;
}

bool LSN::operator!=(const LSN &lsn) const
{
  return val_ != lsn.val_;
}
bool LSN::operator<(const LSN &lsn) const
{
  return val_ < lsn.val_;
}

bool LSN::operator>(const LSN &lsn) const
{
  return lsn < *this;
}

bool LSN::operator>=(const LSN &lsn) const
{
  return lsn < *this || lsn == *this;
}

bool LSN::operator<=(const LSN &lsn) const
{
  return lsn > *this || lsn == *this;
}

LSN& LSN::operator=(const LSN &lsn)
{
  this->val_ = lsn.val_;
  return *this;
}

LSN operator+(const LSN &lsn, const offset_t len)
{
  LSN result;
  result = lsn;
  result.val_ += len;
  return result;
}

LSN operator-(const LSN &lsn, const offset_t len)
{
  LSN result;
  result = lsn;
  result.val_ -= len;
  return result;
}

offset_t operator-(const LSN &lhs, const LSN &rhs)
{
  return lhs.val_ - rhs.val_;
}

DEFINE_SERIALIZE(LSN)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf && buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, val_))) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(LSN)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf && data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, reinterpret_cast<int64_t*>(&val_)))) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(LSN)
{
  int64_t size = 0 ;
  size += serialization::encoded_length_i64(val_);
  return size;
}
} // end namespace palf
} // end namespace oceanbase
