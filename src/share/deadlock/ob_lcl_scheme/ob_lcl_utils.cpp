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

#include "ob_lcl_utils.h"
#include "share/deadlock/ob_deadlock_detector_mgr.h"

namespace oceanbase
{
namespace share
{
namespace detector
{

OB_SERIALIZE_MEMBER(ObLCLLabel, addr_, id_, priority_);

ObLCLLabel::ObLCLLabel(const uint64_t id,
                       const ObDetectorPriority &priority)
  :addr_(GCTX.self_addr()),
  id_(id),
  priority_(priority)
{
  // do nothing
}

ObLCLLabel::ObLCLLabel(const ObLCLLabel &rhs)
  :addr_(rhs.addr_),
  id_(rhs.id_),
  priority_(rhs.priority_)
{
  // do nothing
}

bool ObLCLLabel::is_valid() const
{
  return addr_.is_valid() && priority_.is_valid() && INVALID_VALUE != id_;
}

ObLCLLabel &ObLCLLabel::operator=(const ObLCLLabel &rhs)
{
  addr_ = rhs.addr_;
  id_ = rhs.id_;
  priority_ = rhs.priority_;
  return *this;
}

bool ObLCLLabel::operator==(const ObLCLLabel &rhs) const
{
  return priority_ == rhs.priority_ && addr_ == rhs.addr_ && id_ == rhs.id_;
}

bool ObLCLLabel::operator<(const ObLCLLabel &rhs) const
{
  bool ret = false;
  if (priority_ < rhs.priority_) {
    ret = true;
  } else if (priority_ == rhs.priority_) {
    if (addr_ < rhs.addr_) {
      ret = true;
    } else if (addr_ == rhs.addr_) {
      if (id_ < rhs.id_) {
        ret = true;
      }
    }
  }
  return ret;
}

}
}
}