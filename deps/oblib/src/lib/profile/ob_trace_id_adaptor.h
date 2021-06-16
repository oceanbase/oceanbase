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

namespace oceanbase {
namespace common {

class ObTraceIdAdaptor {
  OB_UNIS_VERSION(1);

public:
  ObTraceIdAdaptor()
  {
    reset();
  }
  void reset();
  const uint64_t* get() const
  {
    return uval_;
  }
  void set(const uint64_t* uval)
  {
    uval_[0] = uval[0];
    uval_[1] = uval[1];
  }
  int64_t to_string(char* buf, const int64_t buf_len) const;
  bool operator==(const ObTraceIdAdaptor& other) const;
  ObTraceIdAdaptor& operator=(const ObTraceIdAdaptor& other);

private:
  uint64_t uval_[2];
};

}  // namespace common
}  // namespace oceanbase

#endif  // OB_COMMON_OB_TRACE_ID_ADAPTOR_H_
