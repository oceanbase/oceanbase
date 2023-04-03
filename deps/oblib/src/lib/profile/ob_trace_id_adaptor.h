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

#ifndef OB_COMMON_OB_TRACE_ID_ADAPTOR_H_
#define OB_COMMON_OB_TRACE_ID_ADAPTOR_H_

#include <stdint.h>
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace common
{

class ObTraceIdAdaptor
{
  OB_UNIS_VERSION(1);

public:
  ObTraceIdAdaptor() { reset(); }
  void reset()
  {
    uval_[0] = 0;
    uval_[1] = 0;
    uval_[2] = 0;
    uval_[3] = 0;
  }
  const uint64_t* get() const { return uval_; }
  void set(const uint64_t *uval) { uval_[0] = uval[0]; uval_[1] = uval[1]; uval_[2] = uval[2]; uval_[3] = uval[3]; }
  int64_t to_string(char *buf, const int64_t buf_len) const;
  bool operator==(const ObTraceIdAdaptor &other) const
  {
    return (uval_[0] == other.uval_[0]) && (uval_[1] == other.uval_[1]) && 
           (uval_[2] == other.uval_[2]) && (uval_[3] == other.uval_[3]);
  }
  ObTraceIdAdaptor &operator=(const ObTraceIdAdaptor &other)
  {
    uval_[0] = other.uval_[0];
    uval_[1] = other.uval_[1];
    uval_[2] = other.uval_[2];
    uval_[3] = other.uval_[3];
    return *this;
  }
private:
  uint64_t uval_[4];
};

}// namespace common
}// namespace oceanbase

#endif //OB_COMMON_OB_TRACE_ID_ADAPTOR_H_

