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

#ifndef OCEANBASE_LIB_OB_INT_FLAGS_
#define OCEANBASE_LIB_OB_INT_FLAGS_

#include <stdint.h>
#include "lib/utility/utility.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
struct ObInt64Flags
{
  ObInt64Flags() : flags_(0)
  { }
  ObInt64Flags(int64_t flags): flags_(flags)
  { }
  virtual ~ObInt64Flags()
  { }

  bool empty() const
  { return 0 == flags_; }

  void reset()
  { flags_ = 0; }

  inline bool add_member(int64_t index);
  inline bool del_member(int64_t index);
  inline bool has_member(int64_t index);

  TO_STRING_KV("flags", PHEX(&flags_, sizeof(flags_)));
private:
  int64_t flags_;
};

bool ObInt64Flags::add_member(int64_t index)
{
  bool bret = true;
  if (index >= 0 && index < static_cast<int64_t>(sizeof(flags_))) {
    flags_ |= 0x1 << index;
  } else {
    bret = false;
  }
  return bret;
}

bool ObInt64Flags::del_member(int64_t index)
{
  bool bret = true;
  if (index >= 0 && index < static_cast<int64_t>(sizeof(flags_))) {
    flags_ &= ~(0x1 << index);
  } else {
    bret = false;
  }
  return bret;
}

bool ObInt64Flags::has_member(int64_t index)
{
  bool bret = false;
  if (index >= 0 && index < static_cast<int64_t>(sizeof(flags_))) {
    bret = flags_ & (0x1 << index);
  } else {
    bret = false;
  }
  return bret;
}

}
}
#endif
