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

#include "ob_deadlock_message.h"

namespace oceanbase
{
namespace share
{
namespace detector
{

using namespace common;

OB_SERIALIZE_MEMBER(ObDeadLockCollectInfoMessage, dest_key_, collected_info_);
OB_SERIALIZE_MEMBER(ObDeadLockNotifyParentMessage, parent_addr_, parent_key_,
                                                   src_addr_, src_key_);

int ObDeadLockCollectInfoMessage::append(const ObDetectorInnerReportInfo &info)
{
  return collected_info_.push_back(info);
}

int ObDeadLockCollectInfoMessage::set_dest_key(const UserBinaryKey &dest_key)
{
  dest_key_ = dest_key;
  return OB_SUCCESS;
}

int ObDeadLockCollectInfoMessage::set_args(const UserBinaryKey &dest_key,
                                           const ObSArray<ObDetectorInnerReportInfo>
                                                 &collected_info)
{
  dest_key_ = dest_key;
  return collected_info_.assign(collected_info);
}

bool ObDeadLockCollectInfoMessage::is_valid() const
{
  return dest_key_.is_valid();
}

int ObDeadLockCollectInfoMessage::assign(const ObDeadLockCollectInfoMessage &rhs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(collected_info_.assign(rhs.collected_info_))) {
    DETECT_LOG(WARN, "fail to copy collected info");
  } else {
    dest_key_ = rhs.dest_key_;
  }
  return ret;
}

const UserBinaryKey &ObDeadLockCollectInfoMessage::get_dest_key() const
{
  return dest_key_;
}

const ObSArray<ObDetectorInnerReportInfo> &ObDeadLockCollectInfoMessage::get_collected_info() const
{
  return collected_info_;
}

int ObDeadLockNotifyParentMessage::set_args(const ObAddr &parent_addr,
                                            const UserBinaryKey &parent_key,
                                            const ObAddr &src_addr,
                                            const UserBinaryKey &src_key)
{
  int ret = OB_SUCCESS;

  if (!parent_addr.is_valid() ||
      !parent_key.is_valid() ||
      !src_addr.is_valid() ||
      !src_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    parent_addr_ = parent_addr;
    parent_key_ = parent_key;
    src_addr_ = src_addr;
    src_key_ = src_key;
  }

  return ret;
}

bool ObDeadLockNotifyParentMessage::is_valid() const
{
  return parent_addr_.is_valid() &&
         parent_key_.is_valid() &&
         src_addr_.is_valid() &&
         src_key_.is_valid();
}

}
}
}