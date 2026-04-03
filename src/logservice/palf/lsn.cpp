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

LSN LSN::atomic_load() const
{
  LSN result;
  const offset_t val = ATOMIC_LOAD(&val_);
  result = LSN(val);
  return result;
}

void LSN::atomic_store(const LSN &lsn)
{
  ATOMIC_STORE(&val_, ATOMIC_LOAD(&lsn.val_));
}

bool LSN::atomic_bcas(const LSN &old_v, const LSN &new_v)
{
  return ATOMIC_BCAS(&val_, old_v.val_, new_v.val_);
}

LSN LSN::atomic_vcas(const LSN &old_v, const LSN &new_v)
{
  const offset_t val = ATOMIC_VCAS(&val_, old_v.val_, new_v.val_);
  LSN result = LSN(val);
  return result;
}

LSN LSN::inc_update(const LSN &ref_lsn)
{
  bool is_updated = false;
  return inc_update(ref_lsn, is_updated);
}

LSN LSN::inc_update(const LSN &ref_lsn, bool &is_updated)
{
  LSN old_lsn;
  is_updated = false;
  LSN new_lsn = this->atomic_load();
  if (false == ref_lsn.is_valid()) {
    // ref_lsn is invalid, return current lsn
  } else {
    while (false == new_lsn.is_valid() || new_lsn < ref_lsn) {
      old_lsn = new_lsn;
      if ((new_lsn = this->atomic_vcas(old_lsn, ref_lsn)) == old_lsn) {
        new_lsn = ref_lsn;
        is_updated = true;
      }
    }
  }
  return new_lsn;
}

LSN LSN::dec_update(const LSN &ref_lsn)
{
  bool is_updated = false;
  return dec_update(ref_lsn, is_updated);
}

// INVALID_LSN_VAL is UINT64_MAX, so we can simply use > to compare
LSN LSN::dec_update(const LSN &ref_lsn, bool &is_updated)
{
  LSN old_lsn;
  LSN new_lsn = this->atomic_load();
  is_updated = false;
  while ((old_lsn = new_lsn) > ref_lsn) {
    if ((new_lsn = this->atomic_vcas(old_lsn, ref_lsn)) == old_lsn) {
      new_lsn = ref_lsn;
      is_updated = true;
    }
  }
  return new_lsn;
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
